Use the Atlassian MCP server already configured in this Codex installation to
retrieve the Jira ticket identified by the Agent Flow structured context.

Do not infer or substitute a different ticket key. Publish the complete tool
result as `jira/ticket.json` and a concise human-readable rendering as
`jira/ticket.md`. If the server is unavailable, authentication is missing, or
the ticket cannot be retrieved, report the failure without fabricating output.
