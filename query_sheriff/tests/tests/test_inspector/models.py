from django.db import models # type: ignore

class InspectorMiddlewareModel(models.Model):
    name = models.CharField(max_length=255)
    email = models.CharField(max_length=100)

    class Meta:
        app_label = 'test_inspector'


class RelatedModel(models.Model):
    # Each RelatedModel instance is related to one InspectorMiddlewareModel,
    # and each InspectorMiddlewareModel can have multiple related models
    inspector_middleware = models.ForeignKey(
        InspectorMiddlewareModel, on_delete=models.CASCADE,
        related_name="related_models", db_index=True)
    related_field = models.CharField(max_length=255)

    class Meta:
        app_label = 'test_inspector'


class AdditionalModel(models.Model):
    related_model = models.ForeignKey(
        RelatedModel, on_delete=models.CASCADE, related_name='additional_models', db_index=False
    )
    name = models.CharField(max_length=255)
    description = models.TextField()

    class Meta:
        app_label = 'test_inspector'


class AnotherRelatedModel(models.Model):
    """
    Another model to test different relationship types and indexing.
    """
    inspector_middleware = models.ForeignKey(
        InspectorMiddlewareModel, on_delete=models.CASCADE,
        related_name="another_related_models", db_index=True
    )
    info_field = models.CharField(max_length=255)
    related_models = models.ManyToManyField(RelatedModel, related_name="many_to_many_related")

    class Meta:
        app_label = 'test_inspector'