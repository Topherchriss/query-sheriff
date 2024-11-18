import logging # type: ignore
from django.db import connection, transaction, models # type: ignore
from django.shortcuts import render # type: ignore
from django.test import RequestFactory # type: ignore
from django.db.models.functions import Length as LengthFunc # type: ignore
from django.db.models import OuterRef, Subquery, Avg, Func, F, Sum, Count, Min, Max # type: ignore
from query_sheriff.inspector.middleware import QueryInspectorMiddleware
from .models import AdditionalModel, AnotherRelatedModel, InspectorMiddlewareModel, InspectorMiddlewareModel, RelatedModel
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse # type: ignore


# Define the view function
def sample_view(request):
    result = InspectorMiddlewareModel.objects.bulk_create([
        InspectorMiddlewareModel(name=f'Batch Item {i}', email=f'item{i}@example.com') for i in range(100)
    ])
    return HttpResponse(f"This is a test view with DB interaction, {len(result)} items created.")


def sample_view_related_model(request):
    inspectors = list(InspectorMiddlewareModel.objects.all()[:100])
    for inspector in inspectors:
        result = RelatedModel.objects.bulk_create([
            RelatedModel(inspector_middleware=inspector, related_field=f'related_{i}_field') for i in range(3)
        ])
    print("Query results", len(result))
    return HttpResponse(f"This is a test view with DB interaction, {len(result)} items created.")


def invalid_query_view(request):
    try:
        # invalid query
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM non_existent_table")
        return HttpResponseBadRequest("This shouldn't be reached due to the invalid query.")
    except Exception as e:
        return HttpResponseBadRequest(f"An error occurred: {str(e)}")


def join_query_view(request):
    # An inner join
    results = RelatedModel.objects.select_related('inspector_middleware').all()
    return HttpResponse(f"Join query completed, found {len(results)} items.")


def subquery_view(request):
    # a subquery fetching a related field from another model
    subquery = RelatedModel.objects.filter(inspector_middleware=OuterRef('pk')).values('inspector_middleware')[:1]
    results = InspectorMiddlewareModel.objects.annotate(related_value=Subquery(subquery))

    return HttpResponse(f"Subquery completed, found {len(results)} items.")

class LengthFunc(Func):
    function = 'LENGTH'

def aggregation_view(request):
    aggregate_results = InspectorMiddlewareModel.objects.aggregate(
        avg_email_length=Avg(LengthFunc('email')),
        sum_id=Sum('id'),
        count_related_field=Count('related_models'),
    )

    # Extract results from the dictionary
    avg_length = aggregate_results['avg_email_length']
    sum_id = aggregate_results['sum_id']

    response = (
        f"Aggregation completed:\n"
        f"- Average email length: {avg_length}\n"
        f"- Sum of IDs: {sum_id}\n" )

    return HttpResponse(response)

def n_plus_one_query_view(request):
    inspectors = InspectorMiddlewareModel.objects.all()

    # N+1 query problem by fetching related models in a loop
    related_fields = []
    for inspector in inspectors:
        related_models = RelatedModel.objects.filter(inspector_middleware=inspector)
        related_fields += [related.related_field for related in related_models]

    return HttpResponse(f"N+1 query completed, found {len(related_fields)} fields.")


def distinct_view(request):
    # Unecessarily DISTINCT
    all_results = InspectorMiddlewareModel.objects.all()
    results = InspectorMiddlewareModel.objects.distinct().filter(related_models__isnull=False)

    return HttpResponse(f"Effcientt distinct triggered, found {len(results)} fields. All results {len(all_results)}")


def inefficient_distinct_view(request):
    # Query using DISTINCT on the primary key (id), which is inherently unique
    distinct_inspectors = InspectorMiddlewareModel.objects.distinct().values('id', 'name')
    data = list(distinct_inspectors)

    return HttpResponse(f"Unnecessary DISTINCT triggered, found {len(data)} fields.")

def missing_limit_view(request):
    results = InspectorMiddlewareModel.objects.all()  # fetches all records without a limit

    return HttpResponse(f"Missing LIMIT clause detected, returned {results.count()} records.")

def inefficient_select_view(request):
    # Query using SELECT *, fetching all fields even if they aren't needed
    results = InspectorMiddlewareModel.objects.all().values()

    return HttpResponse(f"Inefficient SELECT * detected, returned {len(results)} rows.")


def sample_view_another_related_model(request):
    inspectors = list(InspectorMiddlewareModel.objects.all()[:100])
    created_objects = []
    # Create the AnotherRelatedModel objects without assigning related_models
    for inspector in inspectors:
        for i in range(3):
            created_objects.append(
                AnotherRelatedModel(inspector_middleware=inspector, info_field=f'Batch Item {i}')
            )
    # Use bulk_create to insert the objects into the database
    AnotherRelatedModel.objects.bulk_create(created_objects)

    # Assign the related_models to each created AnotherRelatedModel instance
    related_models = list(RelatedModel.objects.all()[:10])  # Fetch related models to assign
    for obj in AnotherRelatedModel.objects.filter(inspector_middleware__in=inspectors):
        obj.related_models.set(related_models)  # Set related models (ManyToMany)
    
    return HttpResponse(f"This is a test view with DB interaction, {len(created_objects)} items created.")


def sample_view_additional_model(request):
    related_models = list(RelatedModel.objects.all()[:100])
    for related_model in related_models:
        result = AdditionalModel.objects.bulk_create([
            AdditionalModel(related_model=related_model, name=f'Additional Batch Item {i}', description=f'item{i}@description') for i in range(2)
        ])
    print("Query results", len(result))
    return HttpResponse(f"This is a test view with DB interaction, {len(result)} items created.")


def detect_inefficient_join_query_view(request):
    """
    View that queries with inefficient JOIN operations.
    """
    # Inefficient query with a JOIN on a non-indexed ForeignKey field
    queryset = RelatedModel.objects.filter(
        additional_models__name__icontains='batch'
    ).select_related('inspector_middleware')
    
    # Inefficient query using nested JOINs and M2M relationship
    queryset = InspectorMiddlewareModel.objects.filter(
        related_models__related_field__icontains='test'
    ).prefetch_related('related_models')

    return HttpResponse(f"Inefficient query executed. found {len(queryset)} items")


def test_multiple_foreign_keys(request):
    """
    View to test querying multiple foreign key columns.
    This will trigger the middleware to check for missing indexes on JOINs.
    """
    
    # Sample data to ensuring related objects
    inspector = InspectorMiddlewareModel.objects.create(name='Inspector 1', email='inspector1@example.com')
    related = RelatedModel.objects.create(inspector_middleware=inspector, related_field='Related 1')
    AdditionalModel.objects.create(related_model=related, name='Additional 1', description='Description 1')
    
    # Querying with multiple foreign keys
    results = AdditionalModel.objects.select_related('related_model__inspector_middleware').filter(
        related_model__inspector_middleware__email='inspector1@example.com'
    )

    # return results (if any)
    data = [{"name": additional.name, "description": additional.description} for additional in results]
    
    return JsonResponse(data, safe=False)

def inefficient_order_by_view(request):
    # Ordering by non-indexed columns: 'name' and 'description'
    additional_model_queryset = (
        AdditionalModel.objects
        .select_related('related_model')
        .order_by('name', 'description', 'related_model__related_field')  # Non-indexed columns
    )
    
    return  HttpResponse(f"Inefficient ORDER BY query executed. found {len(additional_model_queryset)} items")

def overuse_of_subquery_view(request):
    """
    overuse subqueries in the query to trigger inefficiency detection.
    """
    # Subquery to fetch IDs from RelatedModel
    subquery = RelatedModel.objects.filter(inspector_middleware=Subquery(
        InspectorMiddlewareModel.objects.filter(name="Test").values('id')
    )).values('id')

    # Main query uses subquery in WHERE clause inefficiently
    results = InspectorMiddlewareModel.objects.filter(
        id__in=subquery
    ).all()

    return HttpResponse(f"Inefficient use of subqueries has been executed. Found {results}")

def cartesian_product_view(request):
    """
    Perform a JOIN without an ON condition to trigger Cartesian product detection.
    """
    # JOIN without an ON clause
    results = InspectorMiddlewareModel.objects.raw(
        '''
        SELECT * 
        FROM test_inspector_inspectormiddlewaremodel im
        JOIN test_inspector_relatedmodel rm
        ON TRUE  -- Adding ON TRUE to bypass the join condition and trigger a Cartesian product
        '''
    )
    # Force the query to execute by iterating over the results
    count = len(list(results))

    return HttpResponse(f"Cartesian product query has been executed. Found {count} results.")

def inefficient_pagination_view(request):
    """
    use large OFFSET to trigger inefficient pagination detection.
    """
    results = InspectorMiddlewareModel.objects.all()[600:800]

    # Force query execution
    count = len(list(results))

    return HttpResponse(f"Inefficient pagination executed. Retrieved {count} records.")

def non_sargable_query_view(request):
    """
    use a non-sargable query by applying a function on an indexed column in the WHERE clause.
    """
    # Using LOWER() on the indexed 'name' column makes it non-sargable
    results = InspectorMiddlewareModel.objects.raw(
        '''
        SELECT * FROM test_inspector_inspectormiddlewaremodel
        WHERE LOWER(name) = 'test'
        '''
    )

    # Force query execution
    count = len(list(results))

    return HttpResponse(f"Non-sargable query executed. Retrieved {count} records.")

def locking_issue_view(request):
    """
    Perform a long-running query to trigger potential locking issues.
    """
    # Simulate a long-running query using pg_sleep
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_sleep(6);')  # Simulates a 6-second query

    return HttpResponse("Locking issue query executed.")

def overuse_of_transactions_view(request):
    """
    Perform a long transaction to trigger overuse of transactions detection.
    """
    # Start a long transaction block
    with transaction.atomic():
        # Multiple queries in a long transaction block
        for i in range(500):
            InspectorMiddlewareModel.objects.create(name=f"Test {i}", email=f"test{i}@example.com")

    return HttpResponse("Overuse of transactions query executed.")


"""
# Send GET requests to each view using these paths 

### Request to Django test_view endpoint
GET http://127.0.0.1:8000/aggregation-view/ # type: ignore

GET http://127.0.0.1:8000/n-plus-one/ # type: ignore

GET http://127.0.0.1:8000/inefficient-distinct-query # type: ignore

GET http://127.0.0.1:8000/related-view/ # type: ignore

GET http://127.0.0.1:8000/test-view/ # type: ignore

GET http://127.0.0.1:8000/subquery-view/ # type: ignore

GET http://127.0.0.1:8000/join-query/ # type: ignore

GET http://127.0.0.1:8000/distinct-query/ # type: ignore

GET http://127.0.0.1:8000/sample-view-related-model # type: ignore

GET http://127.0.0.1:8000/inefficient-select/ # type: ignore
 
GET http://127.0.0.1:8000/missing-limit/ # type: ignore

GET http://127.0.0.1:8000/sample-view-another-related/ # type: ignore

GET http://127.0.0.1:8000/sample-view-additional-related/ # type: ignore

GET http://127.0.0.1:8000/inefficient-join-query/ # type: ignore

GET http://127.0.0.1:8000/sample-view-another-related/ # type: ignore

GET http://127.0.0.1:8000/multiple-foreign-keys-join-query/ # type: ignore

GET http://127.0.0.1:8000/inefficient-order-by-query/ # type: ignore

GET http://127.0.0.1:8000/overused-subquery-view/ # type: ignore

GET http://127.0.0.1:8000/cartesian-product-view/ # type: ignore

GET http://127.0.0.1:8000/inefficient-pagination-view/ # type: ignore

GET http://127.0.0.1:8000/non-sargable-query-view/ # type: ignore

GET http://127.0.0.1:8000/locking-issue-view/ # type: ignore

GET http://127.0.0.1:8000/overuse-of-transactions-view/ # type: ignore

"""