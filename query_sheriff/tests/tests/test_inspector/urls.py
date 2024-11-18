# urls.py
from django.urls import path # type: ignore
from .views import (sample_view, invalid_query_view, join_query_view,
    sample_view_related_model, subquery_view, aggregation_view,
    n_plus_one_query_view, inefficient_distinct_view, distinct_view,
    inefficient_select_view, missing_limit_view, sample_view_another_related_model,
    sample_view_additional_model, detect_inefficient_join_query_view, test_multiple_foreign_keys,
    inefficient_order_by_view, overuse_of_subquery_view, cartesian_product_view, inefficient_pagination_view,
    non_sargable_query_view, locking_issue_view, overuse_of_transactions_view)

urlpatterns = [
    path('test-view/', sample_view, name='test_view'),
    path('related-view/', sample_view_related_model, name='related_view'),
    path('invalid-query/', invalid_query_view, name='invalid_query_view'),
    path('join-query/', join_query_view, name='join_query_view'),
    path('subquery-view/', subquery_view, name='subquery_view'),
    path('aggregation-view/', aggregation_view, name='aggregation_view'),
    path('n-plus-one/', n_plus_one_query_view, name="n_plus_one_query_view"),
    path('distinct-query/', distinct_view, name='distinct_view'),
    path('inefficient-distinct-query/', inefficient_distinct_view, name='inefficient_distinct_view'),
    path('inefficient-select/', inefficient_select_view, name='inefficient_select_view'),
    path('missing-limit/', missing_limit_view, name='missing_limit_view' ),
    path('sample-view-another-related/', sample_view_another_related_model, name='sample_view_another_related_model'),
    path('sample-view-additional-related/', sample_view_additional_model, name='sample_view_additional_model'),
    path('inefficient-join-query/', detect_inefficient_join_query_view, name='detect_inefficient_join_query_view'),
    path('multiple-foreign-keys-join-query/', test_multiple_foreign_keys, name='test_multiple_foreign_keys'),
    path('inefficient-order-by-query/', inefficient_order_by_view, name='inefficient_order_by_view'),
    path('overused-subquery-view/', overuse_of_subquery_view, name='overuse_of_subquery_view'),
    path('cartesian-product-view/', cartesian_product_view, name='cartesian_product_view'),
    path('inefficient-pagination-view/', inefficient_pagination_view, name='inefficient_pagination_view'),
    path('non-sargable-query-view/', non_sargable_query_view, name='non_sargable_query_view'),
    path('locking-issue-view/', locking_issue_view, name='locking_issue_view'),
    path('overuse-of-transactions-view/', overuse_of_transactions_view, name='overuse_of_transactions_view'),

]
