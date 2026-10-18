
import { Queue } from "bullmq";
import { redis } from "../../infrastructure/redis/redis.js";
import type { FileProcessingJobData } from "./job.types.js";

export const FILE_PROCESSING_QUEUE = "file-processing";

export const fileProcessingQueue = new Queue<FileProcessingJobData>(
  FILE_PROCESSING_QUEUE,
  {
    connection: redis,
    defaultJobOptions: {
      removeOnComplete: {
        age: 3600,
        count: 1000,
      },
      removeOnFail: {
        age: 86400,
        count: 5000,
      },
    },
  },
);
