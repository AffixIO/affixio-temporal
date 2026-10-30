import assert from "node:assert/strict";
import test from "node:test";
import { AffixSDK, createLocalSigningKeyPair, spendingPolicy } from "affixio";
import {
  createAffixTemporalActivityInterceptors,
  TemporalActivityDeniedError,
} from "../dist/index.js";

function context(overrides = {}) {
  return {
    info: {
      activityType: "chargeCard",
      namespace: "payments",
      activityId: "charge-7",
      attempt: 1,
      isLocal: false,
      workflowType: "CheckoutWorkflow",
      workflowExecution: { workflowId: "checkout-42", runId: "run-42" },
      ...overrides,
    },
  };
}

function interceptor(options, ctx = context()) {
  const configured = createAffixTemporalActivityInterceptors(options);
  return configured.activity[0](ctx).inbound;
}

const signingKeys = createLocalSigningKeyPair();

function sdk() {
  return new AffixSDK({
    apiKey: "aio_demo",
    licenceOnly: true,
    autoFlush: false,
    hmacSecret: "temporal-test-secret-that-is-long-enough",
    signingKeys,
  });
}

test("allows a protected activity only after an AffixIO decision and records a redacted receipt", async () => {
  const client = sdk();
  const receipts = [];
  try {
    const inbound = interceptor({
      sdk: client,
      policy: spendingPolicy().currency("GBP").maxPerAction(10).allowCategories(["temporal"]),
      agentId: "agent://checkout-worker",
      protectedActivities: ["chargeCard"],
      amount: 3,
      onDecision: (receipt) => receipts.push(receipt),
    });
    let ran = false;
    const result = await inbound.execute(
      { args: [{ cardToken: "secret-token", amount: 3 }], headers: {} },
      async () => { ran = true; return "charged"; },
    );
    assert.equal(result, "charged");
    assert.equal(ran, true);
    assert.equal(receipts.length, 1);
    assert.equal(receipts[0].allowed, true);
    assert.equal(receipts[0].proofId.startsWith("afx_"), true);
    assert.equal(JSON.stringify(receipts[0]).includes("secret-token"), false);
    assert.match(receipts[0].action.argsDigest, /^[a-f0-9]{64}$/);
  } finally {
    client.dispose();
  }
});

test("fails closed and never executes the activity when policy denies it", async () => {
  const client = sdk();
  try {
    const inbound = interceptor({
      sdk: client,
      policy: spendingPolicy().currency("GBP").maxPerAction(5).allowCategories(["temporal"]),
      agentId: "agent://checkout-worker",
      protectedActivities: ["chargeCard"],
      amount: 9,
    });
    let ran = false;
    await assert.rejects(
      inbound.execute({ args: [{ amount: 9 }], headers: {} }, async () => { ran = true; }),
      (error) => error instanceof TemporalActivityDeniedError && error.receipt.reason === "amount_above_max_per_action",
    );
    assert.equal(ran, false);
  } finally {
    client.dispose();
  }
});

test("does not guard activity names outside the explicit scope", async () => {
  const client = sdk();
  try {
    const inbound = interceptor({
      sdk: client,
      policy: spendingPolicy().currency("GBP").maxPerAction(0).allowCategories(["temporal"]),
      agentId: "agent://checkout-worker",
      protectedActivities: ["chargeCard"],
    }, context({ activityType: "sendEmail" }));
    let ran = false;
    await inbound.execute({ args: ["hello"], headers: {} }, async () => { ran = true; return "sent"; });
    assert.equal(ran, true);
  } finally {
    client.dispose();
  }
});

test("uses the same digest when object keys are ordered differently", async () => {
  const client = sdk();
  const receipts = [];
  try {
    const settings = {
      sdk: client,
      policy: spendingPolicy().currency("GBP").maxPerAction(1).allowCategories(["temporal"]),
      agentId: "agent://checkout-worker",
      protectedActivities: ["chargeCard"],
      onDecision: (receipt) => receipts.push(receipt),
    };
    const inbound = interceptor(settings);
    await inbound.execute({ args: [{ b: 2, a: 1 }], headers: {} }, async () => undefined);
    await inbound.execute({ args: [{ a: 1, b: 2 }], headers: {} }, async () => undefined);
    assert.equal(receipts[0].action.argsDigest, receipts[1].action.argsDigest);
  } finally {
    client.dispose();
  }
});
