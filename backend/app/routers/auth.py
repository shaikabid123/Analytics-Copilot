from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from app.db import get_session
from app.security import AuthConfigError, Principal, create_token, current_user
from app.services.users import authenticate

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class MeOut(BaseModel):
    username: str
    role: str


@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, session: Session = Depends(get_session)) -> TokenOut:
    user = authenticate(session, body.username, body.password)
    if user is None:
        raise HTTPException(401, "invalid credentials", headers={"WWW-Authenticate": "Bearer"})
    try:
        token, ttl = create_token(user.username, user.role.name)
    except AuthConfigError:
        raise HTTPException(503, "authentication is not configured") from None
    return TokenOut(access_token=token, expires_in=ttl)


@router.get("/me", response_model=MeOut)
def me(user: Principal = Depends(current_user)) -> MeOut:
    return MeOut(username=user.username, role=user.role)
