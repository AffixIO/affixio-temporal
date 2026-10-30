import { createHash } from "node:crypto";
import type { Context as ActivityContext } from "@temporalio/activity";
import type {
  ActivityInterceptorsFactory,
  WorkerInterceptors,
} from "@temporalio/worker";
import {
  AffixSDK,
  mcpToolGate,
  type AgenticPaymentPolicy,
} from "affixio";

/** A deliberately small, non-secret description of the activity being checked. */
export type TemporalAction = Readonly<{
  activityType: string;
  workflowType?: string;
  workflowId?: string;
  runId?: string;
  namespace: string;
  activityId: string;
  attempt: number;
  isLocal: boolean;
  argsDigest: string;
}>;

export type TemporalDecisionReceipt = Readonly<{
  allowed: boolean;
  reason: string;
  proofId: string | null;
  proofDigest: string | null;
  decision: string;
  action: TemporalAction;
}>;

export type TemporalAffixOptions = Readonly<{
  /** AffixIO signs the authorization result. Keep this instance server-side. */
  sdk: AffixSDK;
  /** A scoped AffixIO policy. For example, allow category `temporal`. */
  policy: AgenticPaymentPolicy | Record<string, unknown>;
  /** Stable identifier for the actor whose activity is being authorised. */
  agentId: string;
  /** Exact Temporal activity names to protect. An empty list protects nothing. */
  protectedActivities: readonly string[];
  /** Cost in policy currency for a protected activity. Defaults to zero. */
  amount?: number | ((action: TemporalAction) => number | Promise<number>);
  /** Policy category supplied to AffixIO. Defaults to `temporal`. */
  category?: string;
  /** Receives a redacted receipt for each protected invocation, including denials. */
  onDecision?: (receipt: TemporalDecisionReceipt) => void | Promise<void>;
}>;

/**
 * Thrown before an Activity handler runs when its configured AffixIO policy
 * does not allow the invocation. It intentionally does not contain arguments,
 * credentials, proofs, or the full Temporal task token.
 */
export class TemporalActivityDeniedError extends Error {
  readonly receipt: TemporalDecisionReceipt;

  constructor(receipt: TemporalDecisionReceipt) {
    super(`AffixIO denied Temporal activity ${receipt.action.activityType}: ${receipt.reason}`);
    this.name = "TemporalActivityDeniedError";
    this.receipt = receipt;
  }
}

/**
 * Builds Temporal Worker activity interceptors. This is opt-in and protects
 * only activity names listed in `protectedActivities`; it does not alter
 * workflow replay, Temporal authentication, retries, or task-queue routing.
 */
export function createAffixTemporalActivityInterceptors(options: TemporalAffixOptions): WorkerInterceptors {
  validateOptions(options);
  const protectedActivities = new Set(options.protectedActivities);
  const factory: ActivityInterceptorsFactory = (ctx) => ({
    inbound: {
      async execute(input, next) {
        const action = createAction(ctx, input.args);
        if (!protectedActivities.has(action.activityType)) return next(input);

        const amount = typeof options.amount === "function"
          ? await options.amount(action)
          : options.amount ?? 0;
        if (!Number.isFinite(amount) || amount < 0) throw new TypeError("amount must be a finite non-negative number");

        const result = await mcpToolGate({
          sdk: options.sdk,
          policy: options.policy,
          agentId: options.agentId,
          tool: `temporal.activity.${action.activityType}`,
          action: "temporal.activity.execute",
          resource: temporalResource(action),
          amount,
          category: options.category ?? "temporal",
          requestId: requestId(action),
          mode: "offline",
          queueForSync: false,
          meta: {
            activity_type: action.activityType,
            workflow_type: action.workflowType ?? "",
            args_digest: action.argsDigest,
          },
        });
        const receipt = decisionReceipt(action, result);
        await options.onDecision?.(receipt);
        if (!result.allowed) throw new TemporalActivityDeniedError(receipt);
        return next(input);
      },
    },
  });
  return { activity: [factory] };
}

function validateOptions(options: TemporalAffixOptions): void {
  if (!(options.sdk instanceof AffixSDK)) throw new TypeError("sdk must be an AffixSDK instance");
  if (!options.agentId.trim()) throw new TypeError("agentId is required");
  if (!Array.isArray(options.protectedActivities) || options.protectedActivities.some((name) => !name.trim())) {
    throw new TypeError("protectedActivities must contain non-empty activity names");
  }
}

function createAction(ctx: ActivityContext, args: readonly unknown[]): TemporalAction {
  const info = ctx.info;
  return {
    activityType: info.activityType,
    workflowType: info.workflowType,
    workflowId: info.workflowExecution?.workflowId,
    runId: info.workflowExecution?.runId,
    namespace: info.namespace,
    activityId: info.activityId,
    attempt: info.attempt,
    isLocal: info.isLocal,
    argsDigest: sha256(stableJson(args)),
  };
}

function temporalResource(action: TemporalAction): string {
  return [
    "temporal://",
    encodeURIComponent(action.namespace),
    "/workflow/",
    encodeURIComponent(action.workflowId ?? "standalone"),
    "/activity/",
    encodeURIComponent(action.activityId),
  ].join("");
}

function requestId(action: TemporalAction): string {
  return `temporal-${sha256(stableJson({
    namespace: action.namespace,
    workflowId: action.workflowId,
    runId: action.runId,
    activityId: action.activityId,
    activityType: action.activityType,
    argsDigest: action.argsDigest,
  })).slice(0, 48)}`;
}

function decisionReceipt(action: TemporalAction, result: Awaited<ReturnType<typeof mcpToolGate>>): TemporalDecisionReceipt {
  return {
    allowed: result.allowed,
    reason: result.reason,
    proofId: result.receipt.proof_id,
    proofDigest: result.receipt.proof_digest,
    decision: result.receipt.decision,
    action,
  };
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function stableJson(value: unknown): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(value, (_key, item: unknown) => {
    if (typeof item === "bigint") return { $bigint: item.toString() };
    if (item && typeof item === "object") {
      if (seen.has(item)) throw new TypeError("Activity arguments must not contain cycles");
      seen.add(item);
      if (Array.isArray(item)) return item;
      return Object.fromEntries(Object.entries(item as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b)));
    }
    if (typeof item === "undefined") return { $undefined: true };
    return item;
  });
}
