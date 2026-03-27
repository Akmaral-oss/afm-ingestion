from fastapi import HTTPException, status

# auth
UserALreadyExistsException = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail="User is aldready exists"
)

InvalidEmailException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Invalid email"
)

IncorrectEmailOrPasswordException = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Not correct email or password"
)

VerificationCodeNotRequestedException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Verification code was not requested",
)

VerificationCodeExpiredException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Verification code expired",
)


InvalidVerificationCodeException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Invalid verification code",
)

RegistrationFlowRequiredException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Use /auth/register/send-code and /auth/register/confirm",
)

def InvalidFieldException(field_name: str):
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid {field_name}",
    )


# email_service
SMTPNotConfiguredException = HTTPException(
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    detail="SMTP is not configured",
)

EmailDeliveryException = HTTPException(
    status_code=status.HTTP_502_BAD_GATEWAY,
    detail="Failed to send email.",
)


# chat
MissingTokenException = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Missing Bearer token",
)

InvalidTokenException = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid token"
)

AdminRoleRequiredException = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Admin role required for chat access",
)

EmptyQuestionException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Question must not be empty",
)


# transactions
InvalidParserTypeException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Unknown parser_type. Use smart_parser, kaspi, kaspi_parser, bank_parser, halyk_parser or transactions_core",
)


#analytics
MissingIdentityFieldsException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="iin_bin/account/name is required",
)