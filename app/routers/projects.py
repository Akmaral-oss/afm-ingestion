from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Project, RawFile
from ..project_context import (
    ProjectContext,
    create_project_for_user,
    ensure_user_active_project,
    get_current_project_context,
    get_user_projects,
)
from ..schemas import ProjectCreateRequest, ProjectListResponse, ProjectOut, RawFileListResponse, RawFileOut

router = APIRouter(prefix="/projects", tags=["Projects"])


def _serialize_projects(items: list[Project], active_project_id: str | None) -> ProjectListResponse:
    return ProjectListResponse(
        items=[
            ProjectOut(
                project_id=project.project_id,
                name=project.name,
                is_active=project.project_id == active_project_id,
            )
            for project in items
        ],
        active_project_id=active_project_id,
    )


@router.get("", response_model=ProjectListResponse)
async def list_projects(ctx: ProjectContext = Depends(get_current_project_context), db: AsyncSession = Depends(get_db)):
    items = await get_user_projects(db, ctx.user)
    return _serialize_projects(items, ctx.project.project_id)


@router.post("", response_model=ProjectListResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    body: ProjectCreateRequest,
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    project = await create_project_for_user(db, ctx.user, body.name)
    ctx.user.active_project_id = project.project_id
    await db.commit()
    await db.refresh(ctx.user)
    items = await get_user_projects(db, ctx.user)
    return _serialize_projects(items, project.project_id)


@router.post("/{project_id}/activate", response_model=ProjectListResponse)
async def activate_project(
    project_id: str,
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    project = (
        await db.execute(
            select(Project).where(
                Project.project_id == project_id,
                Project.owner_user_id == ctx.user.id,
            )
        )
    ).scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    ctx.user.active_project_id = project.project_id
    await db.commit()
    await db.refresh(ctx.user)
    items = await get_user_projects(db, ctx.user)
    return _serialize_projects(items, project.project_id)


@router.delete("/{project_id}", response_model=ProjectListResponse)
async def delete_project(
    project_id: str,
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    items = await get_user_projects(db, ctx.user)
    if len(items) <= 1:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete the last project")

    project = next((item for item in items if item.project_id == project_id), None)
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    replacement = next((item for item in items if item.project_id != project_id), None)
    if replacement is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot select a replacement project")

    if ctx.user.active_project_id == project_id:
        ctx.user.active_project_id = replacement.project_id
        await db.flush()

    await db.execute(
        text("DELETE FROM afm.transaction_upload_meta WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )

    await db.execute(
        text(
            """
            DELETE FROM afm.field_discovery_log f
            USING afm.raw_files rf
            WHERE f.file_id = rf.file_id
              AND rf.project_id = CAST(:project_id AS uuid)
            """
        ),
        {"project_id": project_id},
    )

    await db.execute(
        text(
            """
            DELETE FROM afm.transactions_ext ext
            USING afm.transactions_core tc
            WHERE ext.tx_id = tc.tx_id
              AND tc.project_id = CAST(:project_id AS uuid)
            """
        ),
        {"project_id": project_id},
    )

    await db.execute(
        text("DELETE FROM afm.query_history WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )
    await db.execute(
        text("DELETE FROM afm.esf_records WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )
    await db.execute(
        text("DELETE FROM afm.transactions_core WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )
    await db.execute(
        text("DELETE FROM afm.statements WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )
    await db.execute(
        text("DELETE FROM afm.raw_files WHERE project_id = CAST(:project_id AS uuid)"),
        {"project_id": project_id},
    )
    await db.execute(
        text(
            """
            DELETE FROM afm.projects
            WHERE project_id = CAST(:project_id AS uuid)
              AND owner_user_id = :owner_user_id
            """
        ),
        {"project_id": project_id, "owner_user_id": ctx.user.id},
    )

    await db.commit()
    await db.refresh(ctx.user)
    fresh_items = await get_user_projects(db, ctx.user)
    return _serialize_projects(fresh_items, ctx.user.active_project_id)


@router.get("/{project_id}/files", response_model=RawFileListResponse)
async def list_project_files(
    project_id: str,
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    # Ensure project exists and belongs to user
    stmt = select(Project).where(
        Project.project_id == project_id,
        Project.owner_user_id == ctx.user.id,
    )
    project = (await db.execute(stmt)).scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Fetch files
    stmt = select(RawFile).where(RawFile.project_id == project_id).order_by(RawFile.uploaded_at.desc())
    files = (await db.execute(stmt)).scalars().all()

    return RawFileListResponse(
        items=[
            RawFileOut(
                file_id=str(f.file_id),
                original_filename=f.original_filename,
                uploaded_at=f.uploaded_at.isoformat(),
                source_bank=f.source_bank,
            )
            for f in files
        ]
    )
