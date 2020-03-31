These tests are going to help check the integrity of built databases to an extent.

test_domain_and_range:

    Gets the types of edges that are in the database. And lookup the edge in the biolink lookup service(https://bl-lookup-sri.renci.org).
    There it will try to look for the domain and range of the edge defination, if those are defined. Those will be used to check if proper 
    directionality is used for the types in the built database.