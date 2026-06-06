import { builder } from "solami";

const token = process.env.WS_TOKEN;

const client = await builder().withRpc(token).build();

const ws = client.ws();
console.log(`connected to ${ws.url}`);

const subscriptionId = ws.connection.onSlotChange((slotInfo) => {
  console.log(`slot ${slotInfo.slot} (parent ${slotInfo.parent})`);
});
console.log("subscribed to slot updates, waiting for messages...");

process.on("SIGINT", async () => {
  await ws.connection.removeSlotChangeListener(subscriptionId);
  console.log("\nstream closed");
  process.exit(0);
});
