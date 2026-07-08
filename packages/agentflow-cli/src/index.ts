#!/usr/bin/env node
import { runCli } from "./router";

runCli(process.argv.slice(2)).then((exitCode) => {
  process.exitCode = exitCode;
});
