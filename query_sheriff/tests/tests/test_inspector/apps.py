from django.apps import AppConfig # type: ignore

class TestInspectorConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    #name = "test_inspector"
    name = "query_sheriff.tests.tests.test_inspector"
