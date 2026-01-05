import pytest
from pydantic import TypeAdapter

from benchmark.conftest import MockAuthor, MockBook
from tests import schemas, transmuters


class TestSimpleValidation:
    """
    Compare simple model performance (flat objects, scalar fields only).

    Both Pydantic and NoOpMateria validate/construct with the same fields, 100 objects.
    """

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_pydantic_validate(self, benchmark, simple_author_data: list[dict]):
        """Pure Pydantic: model_validate on simple objects."""
        data = simple_author_data

        def validate_all():
            return [schemas.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_pydantic_list_adapter(self, benchmark, simple_author_data: list[dict]):
        """Pure Pydantic: model_construct (no validation)."""
        data = simple_author_data
        adapter = TypeAdapter(list[schemas.Author])

        def construct_all():
            return adapter.validate_python(data)

        result = benchmark(construct_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_noop_materia_validate(self, benchmark, simple_author_data: list[dict]):
        """NoOpMateria: model_validate on simple transmuters."""
        data = simple_author_data

        def validate_all():
            return [transmuters.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_noop_materia_list_adapter(self, benchmark, simple_author_data: list[dict]):
        """NoOpMateria: model_construct (no validation)."""
        data = simple_author_data
        adapter = TypeAdapter(list[transmuters.Author])

        def construct_all():
            return adapter.validate_python(data)

        result = benchmark(construct_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)


class TestNestedValidation:
    """
    Compare nested model validation/construct (one level of nesting).
    """

    @pytest.mark.benchmark(group="nested-objects")
    def test_pydantic_validate_nested(self, benchmark, nested_book_data: list[dict]):
        """Pure Pydantic: Validates all nested relationships inline."""
        data = nested_book_data

        def validate_all():
            return [schemas.Book.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], schemas.Book)
        assert isinstance(result[0].author, schemas.Author)
        assert isinstance(result[0].publisher, schemas.Publisher)

    @pytest.mark.benchmark(group="nested-objects")
    def test_pydantic_validate_nested_list_adapter(
        self, benchmark, nested_book_data: list[dict]
    ):
        """Pure Pydantic: Validates all nested relationships inline."""
        data = nested_book_data
        adapter = TypeAdapter(list[schemas.Book])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], schemas.Book)
        assert isinstance(result[0].author, schemas.Author)
        assert isinstance(result[0].publisher, schemas.Publisher)

    @pytest.mark.benchmark(group="nested-objects")
    def test_noop_transmuter_validate_nested(
        self, benchmark, nested_book_data: list[dict]
    ):
        data = nested_book_data

        def validate_all():
            return [transmuters.Book.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], transmuters.Book)
        assert isinstance(result[0].author.value, transmuters.Author)
        assert isinstance(result[0].publisher.value, transmuters.Publisher)

    @pytest.mark.benchmark(group="nested-objects")
    def test_noop_transmuter_validate_nested_list_adapter(
        self, benchmark, nested_book_data: list[dict]
    ):
        data = nested_book_data
        adapter = TypeAdapter(list[transmuters.Book])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], transmuters.Book)
        assert isinstance(result[0].author.value, transmuters.Author)
        assert isinstance(result[0].publisher.value, transmuters.Publisher)


class TestDeepNestedValidation:
    """
    Compare deeply nested validation (Author -> Books -> nested objects).

    Tests author data with books that reference back to the same author (circular).
    """

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_pydantic_validate_deep_nested(
        self, benchmark, deep_nested_data: list[dict]
    ):
        """Pure Pydantic: Validates entire object graph."""
        data = deep_nested_data

        def validate_all():
            return [schemas.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], schemas.Author)
        assert len(result[0].books) >= 1
        assert isinstance(result[0].books[0], schemas.Book)
        # Verify circular reference: book.author should be the same as the parent author
        assert result[0].books[0].author.id == result[0].id

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_pydantic_validate_deep_nested_list_adapter(
        self, benchmark, deep_nested_data: list[dict]
    ):
        """Pure Pydantic: Validates entire object graph using TypeAdapter."""
        data = deep_nested_data
        adapter = TypeAdapter(list[schemas.Author])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], schemas.Author)
        assert len(result[0].books) >= 1

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_noop_transmuter_validate_deep_nested(
        self, benchmark, deep_nested_data: list[dict]
    ):
        """NoOpMateria: Validates author with nested books (books are lazy associations)."""
        data = deep_nested_data

        def validate_all():
            return [transmuters.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], transmuters.Author)
        assert len(result[0].books) >= 1
        assert isinstance(result[0].books[0], transmuters.Book)

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_noop_transmuter_validate_deep_nested_list_adapter(
        self, benchmark, deep_nested_data: list[dict]
    ):
        """NoOpMateria: Validates author with nested books using TypeAdapter."""
        data = deep_nested_data
        adapter = TypeAdapter(list[transmuters.Author])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], transmuters.Author)
        assert len(result[0].books) >= 1


class TestModelDumpDict:
    """
    Compare model_dump() (serialization to dict).

    Uses pre-validated models to isolate dump performance.
    """

    @pytest.mark.benchmark(group="dump-dict")
    def test_pydantic_dump(self, benchmark, simple_author_models: list[schemas.Author]):
        """Pure Pydantic: model_dump to dict."""
        models = simple_author_models

        def dump_all():
            return [m.model_dump() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]

    @pytest.mark.benchmark(group="dump-dict")
    def test_noop_transmuter_dump(
        self, benchmark, simple_author_transmuters: list[transmuters.Author]
    ):
        """NoOpMateria: model_dump to dict (excludes relationship fields)."""
        models = simple_author_transmuters

        def dump_all():
            return [m.model_dump(exclude={"books"}) for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]


class TestModelDumpJson:
    """
    Compare model_dump_json() (serialization to JSON string).
    """

    @pytest.mark.benchmark(group="dump-json")
    def test_pydantic_dump_json(
        self, benchmark, simple_author_models: list[schemas.Author]
    ):
        """Pure Pydantic: model_dump_json to JSON string."""
        models = simple_author_models

        def dump_all():
            return [m.model_dump_json() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="dump-json")
    def test_noop_transmuter_dump_json(
        self, benchmark, simple_author_transmuters: list[transmuters.Author]
    ):
        """NoOpMateria: model_dump_json to JSON string."""
        models = simple_author_transmuters

        def dump_all():
            return [m.model_dump_json(exclude={"books"}) for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100


class TestFromAttributesSimple:
    """
    Compare from_attributes=True pattern (ORM-style attribute access).
    """

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_pydantic_from_attributes(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Pure Pydantic: model_validate with from_attributes=True."""
        objects = mock_author_objects

        def validate_all():
            return [schemas.Author.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_pydantic_from_attributes_list_adapter(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Pure Pydantic: TypeAdapter with from_attributes=True."""
        objects = mock_author_objects
        adapter = TypeAdapter(list[schemas.Author])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_noop_transmuter_from_attributes(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """NoOpMateria: model_validate with from_attributes=True."""
        objects = mock_author_objects

        def validate_all():
            return [transmuters.Author.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_noop_transmuter_from_attributes_list_adapter(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """NoOpMateria: TypeAdapter with from_attributes=True."""
        objects = mock_author_objects
        adapter = TypeAdapter(list[transmuters.Author])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)


class TestFromAttributesNested:
    """
    Compare from_attributes pattern with nested objects.
    """

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_pydantic_nested_from_attributes(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Pure Pydantic: Validates nested attributes from mock objects."""
        objects = mock_nested_book_objects

        def validate_all():
            return [schemas.Book.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Book)
        assert isinstance(result[0].author, schemas.Author)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_pydantic_nested_from_attributes_list_adapter(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Pure Pydantic: TypeAdapter validates nested attributes from mock objects."""
        objects = mock_nested_book_objects
        adapter = TypeAdapter(list[schemas.Book])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Book)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_noop_transmuter_nested_from_attributes(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """NoOpMateria: Validates nested attributes from mock objects."""
        objects = mock_nested_book_objects

        def validate_all():
            return [transmuters.Book.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Book)
        assert isinstance(result[0].author.value, transmuters.Author)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_noop_transmuter_nested_from_attributes_list_adapter(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """NoOpMateria: TypeAdapter validates nested attributes from mock objects."""
        objects = mock_nested_book_objects
        adapter = TypeAdapter(list[transmuters.Book])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Book)
