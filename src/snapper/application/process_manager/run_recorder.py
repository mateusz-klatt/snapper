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
from sqlalchemy import update

from snapper.application.process_manager.enums import ProcessRunStatusEnum
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.config.settings import AppSettings
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ProcessRun
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.messaging.infrastructure.publisher import SequenceTracker


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
        self._tracker = SequenceTracker()

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
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence("db.process_runs"),
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
        now = datetime.now(UTC)
        async with repository.session() as session:
            result_row = await session.execute(
                select(ProcessRun).where(
                    ProcessRun.public_id == public_id,
                    *where_active(ProcessRun),
                )
            )
            process_run = result_row.scalar_one_or_none()
            if process_run is None:
                logger.warning("Process run '{}' not found for status update", public_id)
                return
            await session.execute(
                update(ProcessRun).where(ProcessRun.id == process_run.id).values(known_to=now)
            )
            new_run = ProcessRun(
                public_id=process_run.public_id,
                process_name=process_run.process_name,
                role=process_run.role,
                lifecycle=process_run.lifecycle,
                status=status.value,
                parameters=process_run.parameters,
                result=result if result is not None else process_run.result,
                error=error[:1024] if error is not None else process_run.error,
                tags=process_run.tags,
                started_at=process_run.started_at,
                completed_at=datetime.now(UTC),
                timestamp=now,
                known_to=KNOWN_TO_MAX,
                session_id=process_run.session_id,
                sequence_id=self._tracker.next_sequence("db.process_runs"),
            )
            session.add(new_run)
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
            pr_ts, pr_kt = where_active(ProcessRun)
            stmt = (
                select(ProcessRun)
                .where(pr_ts, pr_kt)
                .order_by(desc(ProcessRun.started_at))
                .limit(limit)
            )
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
