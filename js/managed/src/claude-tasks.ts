import type { Tool, Agent as ClaudeAgent, Options as ClaudeOptions } from "../../nanocodex/runtime/claude.mjs";
import type { ToolContext, AgentEvent } from "nanocodex";
import { stripParentReservation } from "./claude-lifecycle.mjs";

type Descriptor = { agentId: string; parentAgentId: string | null; role: string; task: string; sessionId: string };
/** Blocking native Task delegations use real Claude sessions, not Codex spawning. */
export function managedClaudeTasks(options: {
  storage: DurableObjectStorage;
  create(input: ClaudeOptions): Promise<ClaudeAgent>;
  authorize(context: ToolContext): void;
  bind(descriptor: Descriptor, hostContextRef: string, rootSessionId: string): void;
  release(descriptor: Descriptor, hostContextRef: string, rootSessionId: string): void;
  event(event: AgentEvent, rootSessionId: string, agentId: number): void;
  sessionId(): string;
  uuid(): string;
  concurrency: number;
}) {
  options.storage.sql.exec(`CREATE TABLE IF NOT EXISTS managed_claude_tasks (
    call_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, agent_id INTEGER NOT NULL UNIQUE,
    state TEXT NOT NULL, result TEXT, descriptor_json TEXT NOT NULL
  )`);
  const active = new Map<string, ClaudeAgent>();
  let closing = false;
  let configuration: ClaudeOptions | undefined;
  const properties = { prompt: { type: "string" }, subagent_type: { type: "string" }, model: { type: "string", enum: ["claude-sonnet-4-6", "claude-opus-4-6", "claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-5-5"] } };
  const tools: Tool[] = [
    { name: "Task", description: "Delegate a bounded task to a real Claude child sharing the current authorized tools. This call blocks until completion. Parent cancellation cancels the child. The provider/model is pinned; cross-provider overrides are unsupported.", inputSchema: { type: "object", properties, required: ["prompt", "subagent_type"], additionalProperties: false },
      handler: async (raw, context) => {
        options.authorize(context);
        if (closing || !configuration || active.size >= options.concurrency) throw new Error("Claude child capacity unavailable");
        const input = raw as { prompt?: unknown; subagent_type?: unknown; model?: unknown };
        if (typeof input?.prompt !== "string" || !input.prompt.trim() || input.prompt.length > 65536 || typeof input.subagent_type !== "string" || input.subagent_type.length > 4096 || (input.model !== undefined && input.model !== configuration.model)) throw new Error("invalid Claude Task");
        const previous = options.storage.sql.exec<{ state: string; result: string | null; descriptor_json: string }>("SELECT state,result,descriptor_json FROM managed_claude_tasks WHERE call_id = ?", context.callId).toArray()[0];
        if (previous) {
          const retained = JSON.parse(previous.descriptor_json) as Descriptor;
          if (retained.task !== input.prompt || retained.role !== input.subagent_type || retained.parentAgentId !== (context.subagent?.agentId ?? null)) throw new Error("Claude Task call identity is bound to another delegation");
          if (previous.state === "completed") return { content: previous.result ?? "", isError: false };
          throw new Error("Claude Task outcome is uncertain; refusing to repeat effects");
        }
        const id = options.storage.sql.exec<{ n: number }>("SELECT COALESCE(MAX(agent_id),0)+1 AS n FROM managed_claude_tasks").one().n;
        const descriptor: Descriptor = { agentId: String(id), parentAgentId: context.subagent?.agentId ?? null, role: input.subagent_type, task: input.prompt, sessionId: options.uuid() };
        const hostContextRef = context.turnId;
        if (!hostContextRef) throw new Error("Task requires turn authority");
        const rootSessionId = options.sessionId();
        options.bind(descriptor, hostContextRef, rootSessionId);
        options.storage.sql.exec("INSERT INTO managed_claude_tasks VALUES (?,?,?,'running',NULL,?)", context.callId, descriptor.sessionId, id, JSON.stringify(descriptor));
        let child: ClaudeAgent | undefined;
        let off: (() => void) | undefined;
        let watcher: ReturnType<ClaudeAgent["events"]["watch"]> | undefined;
        const abort = () => { void child?.session.cancel().catch(() => {}); };
        try {
          context.signal.throwIfAborted();
          const childTools = configuration.tools?.map(tool => ({ ...tool, handler: (raw: unknown, inner: ToolContext) => tool.handler(raw, { ...inner, turnId: hostContextRef, subagent: descriptor, signal: AbortSignal.any([inner.signal, context.signal]) }) }));
          if (!configuration.durability) throw new Error("Claude tasks require durable session storage");
          const childOptions = { ...configuration, durability: configuration.durability, sessionId: descriptor.sessionId, durabilityId: descriptor.sessionId, tools: childTools };
          // A child's durable state has its own runtime lifecycle. Never copy
          // the Cloudflare parent's reservation: failing/releasing that child
          // would release the still-running parent's authority and host.
          stripParentReservation(childOptions);
          child = await options.create(childOptions);
          active.set(context.callId, child);
          context.signal.addEventListener("abort", abort, { once: true });
          context.signal.throwIfAborted();
          watcher = child.events.watch();
          off = watcher.onEvent(event => options.event(event, rootSessionId, id));
          const result = await child.turn.prompt({ input: input.prompt, id: `task:${context.callId}` }).result();
          const finalMessage = result.finalMessage;
          result.dispose();
          options.storage.sql.exec("UPDATE managed_claude_tasks SET state='completed',result=? WHERE call_id=?", finalMessage, context.callId);
          return { content: finalMessage, isError: false, structuredResult: { task_id: context.callId, state: "completed" } };
        } catch (error) {
          options.storage.sql.exec("UPDATE managed_claude_tasks SET state='interrupted' WHERE call_id=?", context.callId);
          throw error;
        } finally {
          context.signal.removeEventListener("abort", abort);
          await child?.session.shutdown().catch(() => {});
          child?.dispose();
          off?.(); watcher?.off();
          active.delete(context.callId);
          options.release(descriptor, hostContextRef, rootSessionId);
        }
      } },
    { name: "TaskOutput", description: "Read a retained Claude Task receipt. Running/interrupted tasks are never silently retried after a restart.", inputSchema: { type: "object", properties: { task_id: { type: "string" } }, required: ["task_id"], additionalProperties: false }, handler: raw => {
      const input = raw as { task_id?: unknown }; if (typeof input?.task_id !== "string") throw new Error("task_id required");
      const row = options.storage.sql.exec<{ state: string; result: string | null }>("SELECT state,result FROM managed_claude_tasks WHERE call_id=?", input.task_id).toArray()[0];
      if (!row) throw new Error("unknown task");
      return { content: JSON.stringify({ task_id: input.task_id, state: row.state === "running" && !active.has(input.task_id) ? "interrupted" : row.state, result: row.result }), isError: false };
    } },
    { name: "TaskStop", description: "Cancel a currently running Claude Task; this does not undo completed external actions.", inputSchema: { type: "object", properties: { task_id: { type: "string" } }, required: ["task_id"], additionalProperties: false }, handler: async raw => {
      const input = raw as { task_id?: unknown }; if (typeof input?.task_id !== "string") throw new Error("task_id required"); const child = active.get(input.task_id); if (!child) throw new Error("Task is not running"); await child.session.cancel(); return { content: "Cancellation requested", isError: false };
    } },
  ];
  return { tools: tools.map(tool => ({ ...tool, handler: (input: unknown, context: ToolContext) => { options.authorize(context); context.signal.throwIfAborted(); return tool.handler(input, context); } })), configure(input: ClaudeOptions) { configuration = input; }, async close() { closing = true; await Promise.all([...active.values()].map(async child => { await child.session.cancel().catch(() => {}); await child.session.shutdown().catch(() => {}); })); } };
}
