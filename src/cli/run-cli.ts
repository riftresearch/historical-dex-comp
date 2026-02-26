import {
  handleDashboard,
  handleHeal,
  handleIngest,
  handleIngestLoop,
  handleIntegrity,
  handleMigrate,
  handleRepair,
  handleRuns,
  handleStatus,
} from "./handlers";
import { printUsage } from "./usage";

export async function runCli(args: string[]): Promise<void> {
  const command = args[0];
  const commandArgs = args.slice(1);

  if (!command || command === "--help" || command === "-h") {
    printUsage();
    return;
  }

  if (command === "migrate") {
    await handleMigrate(commandArgs);
    return;
  }

  if (command === "ingest") {
    await handleIngest(commandArgs);
    return;
  }

  if (command === "ingest-loop") {
    await handleIngestLoop(commandArgs);
    return;
  }

  if (command === "status") {
    await handleStatus(commandArgs);
    return;
  }

  if (command === "runs") {
    await handleRuns(commandArgs);
    return;
  }

  if (command === "repair") {
    await handleRepair(commandArgs);
    return;
  }

  if (command === "heal") {
    await handleHeal(commandArgs);
    return;
  }

  if (command === "integrity") {
    await handleIntegrity(commandArgs);
    return;
  }

  if (command === "dashboard") {
    await handleDashboard(commandArgs);
    return;
  }

  throw new Error(`Unknown command '${command}'.`);
}

