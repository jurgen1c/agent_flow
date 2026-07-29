# Releasing Agent Flow

Agent Flow is released independently from Agent Core, Agent Memory, and
Agentic Development.

## Release checklist

1. Work from a clean `main` branch.
2. Confirm the chosen version does not already exist:

   ```bash
   npm view @jurgen1c/agent-flow versions --json
   ```

3. Update `package.json` and run:

   ```bash
   bun install --frozen-lockfile
   bun run ci
   bun run verify:package
   dist/agent-flow.js help
   dist/agent-flow.js --version
   ```

4. Commit and push `main`.
5. Create and push the matching annotated `vX.Y.Z` tag.
6. Publish a GitHub Release for that tag.
7. Wait for the `Publish package` workflow.
8. Verify the registry artifact:

   ```bash
   npm view @jurgen1c/agent-flow version
   ```

Trusted Publishing must authorize repository `jurgen1c/agent_flow`, workflow
`.github/workflows/publish.yml`, with no environment unless the workflow is
updated to use one.
