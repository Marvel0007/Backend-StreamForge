import type { FastifyPluginAsync } from "fastify";
import { getJobSchema, updateJobStatusSchema } from "./job.schema.js";
import { toJobResponse } from "./job.mapper.js";
import {
  getFileProcessingQueueStats,
  getFileProcessingQueueStatus,
  pauseFileProcessingQueue,
  resumeFileProcessingQueue,
} from "../../infrastructure/queue/queue.service.js";
import {
  changeJobStatus,
  getJobById,
  getJobsByFileId,
  retryJob,
  cancelJob,
} from "./job.service.js";
interface GetJobParams {
  id: string;
}

export const jobRoutes: FastifyPluginAsync = async (app) => {
  app.get<{ Params: GetJobParams }>(
    "/api/v1/jobs/:id",
    {
      schema: getJobSchema,
    },
    async (request, reply) => {
      const job = await getJobById(request.params.id);

      return reply.send(toJobResponse(job));
    },
  );
  app.get<{
    Params: {
      fileId: string;
    };
  }>("/api/v1/jobs/file/:fileId", async (request, reply) => {
    const jobs = await getJobsByFileId(request.params.fileId);

    return reply.send({
      data: jobs.map(toJobResponse),
    });
  });
  app.get("/api/v1/jobs/queue/stats", async (_request, reply) => {
    const stats = await getFileProcessingQueueStats();

    return reply.send({
      data: stats,
    });
  });
  app.get("/api/v1/jobs/queue/status", async (_request, reply) => {
    const status = await getFileProcessingQueueStatus();

    return reply.send({
      data: status,
    });
  });
  app.post<{ Params: { id: string } }>(
    "/api/v1/jobs/:id/cancel",
    async (request, reply) => {
      const job = await cancelJob(request.params.id);

      return reply.send({
        data: toJobResponse(job),
      });
    },
  );
  app.post<{ Params: { id: string } }>(
    "/api/v1/jobs/:id/retry",
    async (request, reply) => {
      const job = await retryJob(request.params.id);

      return reply.send({
        data: toJobResponse(job),
      });
    },
  );
  app.post("/api/v1/jobs/queue/pause", async (_request, reply) => {
    await pauseFileProcessingQueue();

    return reply.send({
      message: "File processing queue paused",
    });
  });
  app.post("/api/v1/jobs/queue/resume", async (_request, reply) => {
    await resumeFileProcessingQueue();

    return reply.send({
      message: "File processing queue resumed",
    });
  });
  app.patch<{
    Params: {
      id: string;
    };
    Body: {
      status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED";
    };
  }>(
    "/api/v1/jobs/:id/status",
    {
      schema: updateJobStatusSchema,
    },
    async (request, reply) => {
      const job = await changeJobStatus(request.params.id, request.body.status);

      return reply.send({
        data: toJobResponse(job),
      });
    },
  );
};

// cmtgdzf9n000bkgri5697ucbm
