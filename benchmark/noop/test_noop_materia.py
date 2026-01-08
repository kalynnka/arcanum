import pytest
from pydantic import TypeAdapter

from benchmark.conftest import MockAuthor, MockBook
from tests import schemas, transmuters


class TestSimpleValidation:
    """
    Compare simple model performance (flat objects, scalar fields only).

    Both Pydantic and NoOpMateria validate/construct with the same fields, 100 objects.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_pydantic_scalar_validate(self, benchmark, simple_author_data: list[dict]):
        """Pure Pydantic: model_validate on simple objects."""
        data = simple_author_data

        def validate_all():
            return [schemas.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_pydantic_scalar_adapter(self, benchmark, simple_author_data: list[dict]):
        """Pure Pydantic: TypeAdapter validation."""
        data = simple_author_data
        adapter = TypeAdapter(list[schemas.Author])

        def construct_all():
            return adapter.validate_python(data)

        result = benchmark(construct_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_transmuter_scalar_validate(
        self, benchmark, simple_author_data: list[dict]
    ):
        """Arcanus: model_validate on transmuters."""
        data = simple_author_data

        def validate_all():
            return [transmuters.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)

    @pytest.mark.benchmark(group="scalars-only-objects")
    def test_transmuter_scalar_adapter(self, benchmark, simple_author_data: list[dict]):
        """Arcanus: TypeAdapter validation."""
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

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="nested-objects")
    def test_pydantic_nested_validate(self, benchmark, nested_book_data: list[dict]):
        """Pure Pydantic: Validates nested relationships."""
        data = nested_book_data

        def validate_all():
            return [schemas.Book.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], schemas.Book)
        assert isinstance(result[0].author, schemas.Author)
        assert isinstance(result[0].publisher, schemas.Publisher)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="nested-objects")
    def test_pydantic_nested_adapter(self, benchmark, nested_book_data: list[dict]):
        """Pure Pydantic: TypeAdapter validation."""
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
    def test_transmuter_nested_validate(self, benchmark, nested_book_data: list[dict]):
        """Arcanus: Validates nested relationships."""
        data = nested_book_data

        def validate_all():
            return [transmuters.Book.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 50
        assert isinstance(result[0], transmuters.Book)
        assert isinstance(result[0].author.value, transmuters.Author)
        assert isinstance(result[0].publisher.value, transmuters.Publisher)

    @pytest.mark.benchmark(group="nested-objects")
    def test_transmuter_nested_adapter(self, benchmark, nested_book_data: list[dict]):
        """Arcanus: TypeAdapter validation."""
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

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_pydantic_deep_validate(self, benchmark, deep_nested_data: list[dict]):
        """Pure Pydantic: Validates deep object graph."""
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

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_pydantic_deep_adapter(self, benchmark, deep_nested_data: list[dict]):
        """Pure Pydantic: TypeAdapter validation."""
        data = deep_nested_data
        adapter = TypeAdapter(list[schemas.Author])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], schemas.Author)
        assert len(result[0].books) >= 1

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_transmuter_deep_validate(self, benchmark, deep_nested_data: list[dict]):
        """Arcanus: Validates deep object graph."""
        data = deep_nested_data

        def validate_all():
            return [transmuters.Author.model_validate(d) for d in data]

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], transmuters.Author)
        assert len(result[0].books) >= 1
        assert isinstance(result[0].books[0], transmuters.Book)

    @pytest.mark.benchmark(group="deep-nested-objects")
    def test_transmuter_deep_adapter(self, benchmark, deep_nested_data: list[dict]):
        """Arcanus: TypeAdapter validation."""
        data = deep_nested_data
        adapter = TypeAdapter(list[transmuters.Author])

        def validate_all():
            return adapter.validate_python(data)

        result = benchmark(validate_all)
        assert len(result) == 10
        assert isinstance(result[0], transmuters.Author)
        assert len(result[0].books) >= 1


class TestScalarsOnlyDumpDict:
    """
    Compare model_dump() for scalar-only models (no relationships).

    Uses flat models without any association fields to get pure dump performance.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="scalars-only-dump-dict")
    def test_pydantic_scalars_dump_dict(
        self, benchmark, simple_author_flat_models: list[schemas.AuthorFlat]
    ):
        """Pure Pydantic: model_dump to dict (flat model)."""
        models = simple_author_flat_models

        def dump_all():
            return [m.model_dump() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]
        assert "books" not in result[0]

    @pytest.mark.benchmark(group="scalars-only-dump-dict")
    def test_transmuter_scalars_dump_dict(
        self, benchmark, simple_author_flat_transmuters: list[transmuters.AuthorFlat]
    ):
        """Arcanus: model_dump to dict (flat transmuter)."""
        models = simple_author_flat_transmuters

        def dump_all():
            return [m.model_dump() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]
        assert "books" not in result[0]


class TestScalarsOnlyDumpJson:
    """
    Compare model_dump_json() for scalar-only models (no relationships).
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="scalars-only-dump-json")
    def test_pydantic_scalars_dump_json(
        self, benchmark, simple_author_flat_models: list[schemas.AuthorFlat]
    ):
        """Pure Pydantic: model_dump_json (flat model)."""
        models = simple_author_flat_models

        def dump_all():
            return [m.model_dump_json() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="scalars-only-dump-json")
    def test_transmuter_scalars_dump_json(
        self, benchmark, simple_author_flat_transmuters: list[transmuters.AuthorFlat]
    ):
        """Arcanus: model_dump_json (flat transmuter)."""
        models = simple_author_flat_transmuters

        def dump_all():
            return [m.model_dump_json() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100


class TestModelDumpDict:
    """
    Compare model_dump() (serialization to dict).

    Uses pre-validated models to isolate dump performance.
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="dump-dict")
    def test_pydantic_dump_dict(
        self, benchmark, simple_author_models: list[schemas.Author]
    ):
        """Pure Pydantic: model_dump to dict."""
        models = simple_author_models

        def dump_all():
            return [m.model_dump() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100
        assert "name" in result[0]

    @pytest.mark.benchmark(group="dump-dict")
    def test_transmuter_dump_dict(
        self, benchmark, simple_author_transmuters: list[transmuters.Author]
    ):
        """Arcanus: model_dump to dict."""
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

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="dump-json")
    def test_pydantic_dump_json(
        self, benchmark, simple_author_models: list[schemas.Author]
    ):
        """Pure Pydantic: model_dump_json."""
        models = simple_author_models

        def dump_all():
            return [m.model_dump_json() for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100

    @pytest.mark.benchmark(group="dump-json")
    def test_transmuter_dump_json(
        self, benchmark, simple_author_transmuters: list[transmuters.Author]
    ):
        """Arcanus: model_dump_json."""
        models = simple_author_transmuters

        def dump_all():
            return [m.model_dump_json(exclude={"books"}) for m in models]

        result = benchmark(dump_all)
        assert len(result) == 100


class TestFromAttributesSimple:
    """
    Compare from_attributes=True pattern (ORM-style attribute access).
    """

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_pydantic_from_attrs(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Pure Pydantic: from_attributes validation."""
        objects = mock_author_objects

        def validate_all():
            return [schemas.Author.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_pydantic_from_attrs_adapter(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Pure Pydantic: TypeAdapter validation."""
        objects = mock_author_objects
        adapter = TypeAdapter(list[schemas.Author])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Author)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_transmuter_from_attrs(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Arcanus: from_attributes validation."""
        objects = mock_author_objects

        def validate_all():
            return [transmuters.Author.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Author)

    @pytest.mark.benchmark(group="from-attributes-simple")
    def test_transmuter_from_attrs_adapter(
        self, benchmark, mock_author_objects: list[MockAuthor]
    ):
        """Arcanus: TypeAdapter validation."""
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

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_pydantic_from_attrs_nested(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Pure Pydantic: Validates nested attributes."""
        objects = mock_nested_book_objects

        def validate_all():
            return [schemas.Book.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Book)
        assert isinstance(result[0].author, schemas.Author)

    @pytest.mark.baseline
    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_pydantic_from_attrs_nested_adapter(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Pure Pydantic: TypeAdapter validation."""
        objects = mock_nested_book_objects
        adapter = TypeAdapter(list[schemas.Book])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], schemas.Book)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_transmuter_from_attrs_nested(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Arcanus: Validates nested attributes."""
        objects = mock_nested_book_objects

        def validate_all():
            return [transmuters.Book.model_validate(obj) for obj in objects]

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Book)
        assert isinstance(result[0].author.value, transmuters.Author)

    @pytest.mark.benchmark(group="from-attributes-nested")
    def test_transmuter_from_attrs_nested_adapter(
        self, benchmark, mock_nested_book_objects: list[MockBook]
    ):
        """Arcanus: TypeAdapter validation."""
        objects = mock_nested_book_objects
        adapter = TypeAdapter(list[transmuters.Book])

        def validate_all():
            return adapter.validate_python(objects)

        result = benchmark(validate_all)
        assert len(result) == 100
        assert isinstance(result[0], transmuters.Book)
