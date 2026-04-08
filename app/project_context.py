from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_db
from .models import Project, User
from .security import decode_access_token

DEFAULT_PROJECT_NAME = "Project 1"


@dataclass
class ProjectContext:
    payload: dict
    user: User
    project: Project


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def create_project_for_user(
    db: AsyncSession,
    user: User,
    name: str,
) -> Project:
    normalized_name = (name or DEFAULT_PROJECT_NAME).strip() or DEFAULT_PROJECT_NAME
    existing = (
        await db.execute(
            select(Project)
            .where(
                Project.owner_user_id == user.id,
                Project.name == normalized_name,
            )
            .order_by(Project.created_at.asc(), Project.project_id.asc())
        )
    ).scalars().first()
    if existing:
        return existing

    now = _utc_now()
    project = Project(
        project_id=str(uuid.uuid4()),
        owner_user_id=user.id,
        name=normalized_name,
        created_at=now,
        updated_at=now,
    )
    db.add(project)
    await db.flush()
    return project


async def ensure_user_active_project(
    db: AsyncSession,
    user: User,
    *,
    default_project_name: str = DEFAULT_PROJECT_NAME,
) -> Project:
    projects = list(
        (
            await db.execute(
                select(Project)
                .where(Project.owner_user_id == user.id)
                .order_by(Project.created_at.asc(), Project.project_id.asc())
            )
        ).scalars()
    )

    if not projects:
        project = await create_project_for_user(db, user, default_project_name)
        user.active_project_id = project.project_id
        await db.flush()
        return project

    if user.active_project_id:
        for project in projects:
            if project.project_id == user.active_project_id:
                return project

    user.active_project_id = projects[0].project_id
    await db.flush()
    return projects[0]


async def get_user_projects(db: AsyncSession, user: User) -> list[Project]:
    await ensure_user_active_project(db, user)
    return list(
        (
            await db.execute(
                select(Project)
                .where(Project.owner_user_id == user.id)
                .order_by(Project.created_at.asc(), Project.project_id.asc())
            )
        ).scalars()
    )


async def resolve_project_context(
    authorization: Optional[str] = Header(default=None),
    x_project_id: Optional[str] = Header(default=None, alias="X-Project-Id"),
    db: AsyncSession = Depends(get_db),
) -> ProjectContext:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = decode_access_token(token)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    user = (
        await db.execute(
            select(User).where(User.id == int(user_id))
        )
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    active_project = await ensure_user_active_project(db, user)

    if x_project_id and x_project_id != active_project.project_id:
        selected = (
            await db.execute(
                select(Project).where(
                    Project.project_id == x_project_id,
                    Project.owner_user_id == user.id,
                )
            )
        ).scalar_one_or_none()
        if not selected:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
        user.active_project_id = selected.project_id
        await db.commit()
        await db.refresh(user)
        active_project = selected

    return ProjectContext(payload=payload, user=user, project=active_project)


async def get_current_project_context(
    authorization: Optional[str] = Header(default=None),
    x_project_id: Optional[str] = Header(default=None, alias="X-Project-Id"),
    db: AsyncSession = Depends(get_db),
) -> ProjectContext:
    return await resolve_project_context(
        authorization=authorization,
        x_project_id=x_project_id,
        db=db,
    )
