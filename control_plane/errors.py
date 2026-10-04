class DomainError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        super().__init__(message)


class AdmissionPaused(DomainError):
    def __init__(self) -> None:
        super().__init__(503, "new job admission is paused by the platform administrator")
