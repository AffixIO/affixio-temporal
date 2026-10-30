# AffixIO Temporal activity interceptor

`@affixio/temporal` is an opt-in Temporal Worker activity interceptor. It asks
AffixIO to make a scoped policy decision before an explicitly named Activity
runs. A denied decision throws before the Activity handler is called. Each
protected invocation can emit a redacted, signed-decision receipt for the host
application's audit sink.

This is a reference integration, not a Temporal endorsement or certification.
It does not replace Temporal authentication, authorisation, task queues,
workflow retries, idempotency, payment-provider checks, or application-side
transaction handling. The host remains responsible for enforcing decisions.

## Why the activity boundary

Temporal Workflow code must be deterministic. The AffixIO check belongs in a
Worker Activity inbound interceptor, outside the Workflow isolate, immediately
before the Activity's side effect. Workflow code should not call remote policy
or signing services directly.

## Install

```bash
npm install affixio @temporalio/worker @affixio/temporal
```

## Use

```ts
import { Worker } from "@temporalio/worker";
import { AffixSDK, spendingPolicy } from "affixio";
import { createAffixTemporalActivityInterceptors } from "@affixio/temporal";

const sdk = new AffixSDK({
  apiKey: process.env.AFFIX_API_KEY!,
  // Pick a durable server-side store and signing configuration in production.
  operatorBaseDir: "/var/lib/my-service/affix",
  persistSecrets: true,
});

const worker = await Worker.create({
  taskQueue: "payments",
  activities: { chargeCard },
  interceptors: createAffixTemporalActivityInterceptors({
    sdk,
    agentId: "agent://payments-worker",
    protectedActivities: ["chargeCard"],
    policy: spendingPolicy()
      .currency("GBP")
      .maxPerAction(100)
      .allowCategories(["temporal"]),
    amount: (action) => action.activityType === "chargeCard" ? 25 : 0,
    onDecision: async (receipt) => auditStore.append(receipt),
  }),
});
```

`onDecision` receives only metadata and digests. It does not receive Activity
arguments, secrets, the Temporal task token or a raw proof. Treat the audit
sink as part of the security boundary: if it throws, an otherwise allowed
Activity does not run.

## Security notes

- Scope protection with exact `protectedActivities`; this adapter does not
  silently protect every activity.
- The action binds namespace, workflow/run IDs when available, activity ID,
  activity type and a SHA-256 digest of the argument structure. Arguments are
  not included in the AffixIO metadata or receipt.
- Configure a durable, shared replay/idempotency and evidence store where a
  deployment has multiple Worker processes. This example intentionally does
  not claim that an in-memory default provides distributed replay protection.
- Pin verification keys independently when another system verifies AffixIO
  evidence. A local HMAC proof is for trusted server parties, not a public
  proof of truth.
- Run a reconciliation design review before using this around payments. A
  policy decision is not a payment-provider authorisation or settlement proof.

## Test

```bash
npm test
```

The suite executes the interceptor against Temporal's real Worker interceptor
types and AffixIO in offline, licence-only mode. It covers allow, deny-before-
execution, explicit scoping and redacted/stable argument digests. It does not
connect to a Temporal server or send a production action.
