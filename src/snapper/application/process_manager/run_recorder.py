"""Process run record persistence service.

Provides database operations for creating, updating, and querying
process run records that track execution history.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

from loguru import logger
from sqlalchemy import desc
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessRunStatusEnum
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.config.settings import AppSettings
from snapper.data.models import ProcessRun
from snapper.data.repository import get_repository


class ProcessRunRecorder:
    """Persists process run records to the database.

    Handles creating new run records when processes start,
    updating status on completion or failure, and querying
    run history.

    Attributes:
        settings: Application settings for database access.
    """

    def __init__(self, settings: AppSettings) -> None:
        """Initialize the run recorder.

        Args:
            settings: Application settings for database URL.
        """
        self.settings = settings

    async def create_run_record(
        self,
        config: ProcessConfigModel,
        parameters: dict[str, Any] | None,
    ) -> str:
        """Create a new process run record in database.

        Args:
            config: Process configuration.
            parameters: Runtime parameters for this run.

        Returns:
            Generated public_id (UUID string).
        """
        repository = get_repository(self.settings.db_url)
        public_id = str(uuid7())
        async with repository.session() as session:
            run = ProcessRun(
                public_id=public_id,
                process_name=config.name,
                role=config.role.value,
                lifecycle=config.lifecycle.value,
                status=ProcessRunStatusEnum.RUNNING.value,
                parameters=parameters,
                tags=list(config.tags),
                started_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
            )
            session.add(run)
            await session.commit()
        return public_id

    async def update_run_record(
        self,
        public_id: str,
        status: ProcessRunStatusEnum,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Update an existing process run record.

        Args:
            public_id: Public ID to update.
            status: New status to set.
            result: Optional result data dict.
            error: Optional error message (truncated to 1024 chars).
        """
        repository = get_repository(self.settings.db_url)
        async with repository.session() as session:
            result_row = await session.execute(
                select(ProcessRun).where(ProcessRun.public_id == public_id)
            )
            process_run = result_row.scalar_one_or_none()
            if process_run is None:
                logger.warning("Process run '{}' not found for status update", public_id)
                return
            process_run.status = status.value
            process_run.completed_at = datetime.now(UTC)
            if result is not None:
                process_run.result = result
            if error is not None:
                process_run.error = error[:1024]
            await session.commit()

    async def get_recent_runs(
        self,
        *,
        limit: int = 50,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve recent process run history.

        Args:
            limit: Maximum number of runs to return.
            name: Filter by process name.

        Returns:
            List of run records with status, timestamps, and results.
        """
        repository = get_repository(self.settings.db_url)
        async with repository.session() as session:
            stmt = select(ProcessRun).order_by(desc(ProcessRun.started_at)).limit(limit)
            if name:
                stmt = stmt.where(ProcessRun.process_name == name)
            result = await session.execute(stmt)
            runs = result.scalars().all()
        return [
            {
                "public_id": run.public_id,
                "process_name": run.process_name,
                "status": run.status,
                "role": run.role,
                "lifecycle": run.lifecycle,
                "parameters": run.parameters,
                "result": run.result,
                "error": run.error,
                "tags": run.tags or [],
                "started_at": run.started_at.isoformat(),
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            }
            for run in runs
        ]
