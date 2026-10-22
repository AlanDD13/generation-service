import hashlib
import hmac

from fastapi import HTTPException


def sign_body(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(body: bytes, signature: str | None, secret: str) -> None:
    if signature is None or not hmac.compare_digest(
        sign_body(body, secret).encode(), signature.encode()
    ):
        raise HTTPException(401, "Invalid signature")
