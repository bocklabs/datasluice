def call_service_method(service: object, name: str, /, **kwargs: object) -> object:
    """Invoke a typed service method by runtime name so the boundary TypeError is what is observed."""
    return getattr(service, name)(**kwargs)
