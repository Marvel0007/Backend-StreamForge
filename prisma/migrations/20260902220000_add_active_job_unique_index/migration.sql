CREATE UNIQUE INDEX "Job_fileId_active_unique"
ON "Job" ("fileId")
WHERE "status" IN ('PENDING', 'PROCESSING');