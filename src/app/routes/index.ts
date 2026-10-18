import type { FastifyPluginAsync } from "fastify";
import { fileRoutes } from "../../modules/files/file.routes.js";
import { jobRoutes } from "../../modules/jobs/job.routes.js";
import { storageRoutes } from "../../infrastructure/storage/storage.routes.js";

export const registerRoutes: FastifyPluginAsync = async (app) => {
  app.get("/api/v1/health", async () => {
    return {
      status: "ok",
      service: "streamforge",
      version: "v1",
    };
  });

  await app.register(fileRoutes);
  await app.register(jobRoutes);
  app.register(storageRoutes);
};

