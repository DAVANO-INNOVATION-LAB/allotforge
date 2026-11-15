# GitHub agent setup

Three manually selected profiles for `allotforge` are in `.github/agents/`: build engineer (`devops-platform`), test engineer (`test-engineer`) and read-only security reviewer (`devops-security`). They reuse existing engineering responsibilities without copying private coordination records or enabling the unrelated business/finance workers.

Select a custom agent in GitHub Copilot for a bounded task after these files are available on the intended branch. Profiles do not grant an entitlement, activate a seat, start continuous execution, or enable paid usage. Repository policy currently permits Copilot, but organization seat access is not established by these files. Existing CI remains unchanged.

No secrets, new MCP servers, schedules, workflow permissions or publishing rights are configured. Read the existing CI and instructions for exact build/test commands. The security profile has read/search tools only. Build/test execute capability is limited by GitHub's runtime controls; prose instructions are not an OS sandbox.

Documentation: https://docs.github.com/en/copilot/reference/custom-agents-configuration
