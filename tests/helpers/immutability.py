def assign_readonly(target: object, field: str, value: object) -> None:
    """Assign a read-only field so the frozen-dataclass rejection is what the assertion observes."""
    setattr(target, field, value)
