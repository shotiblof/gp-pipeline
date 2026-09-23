/** gp-cron: GHA dispatch for shotiblof/gp-pipeline (gayporno → namevids). */
export interface Env {
  GITHUB_DISPATCH_TOKEN: string;
  GITHUB_REPO?: string;
  CRON_SECRET?: string;
}

const DEFAULT_REPO = "shotiblof/gp-pipeline";

/** Offset vs peach-cron on the same 5-minute grid (:00,:05,:10,...). */
function workflowsForMinute(minute: number): string[] {
  const jobs: string[] = [];
  // peach latest is %15===0; sy latest uses :05/:20/:35/:50
  if (minute % 15 === 5) jobs.push("parser-latest.yml");
  // peach backlog is :05/:25/:45; sy backlog :00/:30
  if (minute === 0 || minute === 30) jobs.push("parser-backlog.yml");
  // peach uploader is %15===10; sy uploader :00/:15/:30/:45
  if (minute % 15 === 0) jobs.push("uploader.yml");
  return jobs;
}

async function workflowBusy(env: Env, workflowFile: string): Promise<boolean> {
  const repo = (env.GITHUB_REPO || DEFAULT_REPO).trim();
  const token = env.GITHUB_DISPATCH_TOKEN?.trim();
  if (!token) return false;

  for (const status of ["in_progress", "queued", "waiting", "pending", "requested"] as const) {
    const response = await fetch(
      `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/runs?status=${status}&per_page=1`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
          "User-Agent": "gp-cron-worker",
        },
      },
    );
    if (!response.ok) continue;
    const data = (await response.json()) as { total_count?: number };
    if ((data.total_count ?? 0) > 0) return true;
  }
  return false;
}

async function dispatchWorkflow(env: Env, workflowFile: string): Promise<string> {
  const repo = (env.GITHUB_REPO || DEFAULT_REPO).trim();
  const token = env.GITHUB_DISPATCH_TOKEN?.trim();
  if (!token) {
    throw new Error("GITHUB_DISPATCH_TOKEN is not configured");
  }

  const response = await fetch(
    `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "gp-cron-worker",
      },
      body: JSON.stringify({ ref: "main" }),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`dispatch ${workflowFile} failed: ${response.status} ${detail}`);
  }

  return workflowFile;
}

function requireCronSecret(request: Request, env: Env): Response | null {
  const secret = env.CRON_SECRET?.trim();
  const provided = request.headers.get("X-Cron-Secret")?.trim();
  if (!secret || !provided || secret !== provided) {
    return new Response("Unauthorized", { status: 401 });
  }
  return null;
}

export default {
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    const minute = new Date(event.scheduledTime).getUTCMinutes();
    const repo = env.GITHUB_REPO || DEFAULT_REPO;
    const workflows = workflowsForMinute(minute);
    if (workflows.length === 0) {
      console.log(`scheduled: minute ${minute} — no jobs`);
      return;
    }

    console.log(`scheduled: ${repo} minute ${minute} → ${workflows.join(", ")}`);
    for (const workflow of workflows) {
      try {
        if (workflow === "uploader.yml" && (await workflowBusy(env, workflow))) {
          console.log(`scheduled: skip ${workflow} — already running or queued`);
          continue;
        }
        await dispatchWorkflow(env, workflow);
        console.log(`scheduled: dispatched ${workflow}`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`scheduled: ${workflow} failed: ${message}`);
      }
    }
  },

  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({
        status: "ok",
        worker: "gp-cron",
        repo: env.GITHUB_REPO || DEFAULT_REPO,
      });
    }

    if (url.pathname === "/trigger" && request.method === "POST") {
      const auth = requireCronSecret(request, env);
      if (auth) return auth;

      const job = url.searchParams.get("job")?.trim() || "";
      try {
        if (job === "parser-latest") {
          await dispatchWorkflow(env, "parser-latest.yml");
        } else if (job === "parser-backlog") {
          await dispatchWorkflow(env, "parser-backlog.yml");
        } else if (job === "uploader") {
          await dispatchWorkflow(env, "uploader.yml");
        } else if (job === "all") {
          await dispatchWorkflow(env, "parser-latest.yml");
          await dispatchWorkflow(env, "parser-backlog.yml");
          await dispatchWorkflow(env, "uploader.yml");
        } else {
          return Response.json({ error: "unknown job" }, { status: 400 });
        }
        return Response.json({ ok: true, job, repo: env.GITHUB_REPO || DEFAULT_REPO });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return Response.json({ ok: false, error: message }, { status: 502 });
      }
    }

    return new Response("Not found", { status: 404 });
  },
};
