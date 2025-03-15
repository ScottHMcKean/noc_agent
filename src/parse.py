from pydantic import BaseModel
from typing import Type
from docling_core.types.doc import TextItem, SectionHeaderItem, DocItemLabel
from docling_core.types.doc.document import DoclingDocument


def get_hierarchy_level(text: str) -> tuple[int, str | None]:
    """
    Determine the hierarchy level of a text based on its numeric prefix.

    Args:
        text (str): The input text to analyze

    Returns:
        tuple[int, str | None]: A tuple containing:
            - hierarchy level (1-4, or 0 if no numeric prefix)
            - the numeric prefix if found, None otherwise

    Examples:
        >>> get_hierarchy_level("52 Technical occupations")      # Returns (1, "52")
        >>> get_hierarchy_level("5210 Technical occupations")    # Returns (3, "5210")
        >>> get_hierarchy_level("No numbers here")               # Returns (0, None)
    """
    text = text.strip()
    # Find the numeric prefix
    numeric_prefix = ""
    for char in text:
        if char.isdigit():
            numeric_prefix += char
        else:
            break

    if not numeric_prefix:
        return (0, None)

    # Check that there are at least 5 non-numeric characters after the prefix
    remaining_text = text[len(numeric_prefix) :].strip()
    if len(remaining_text) < 5:
        return (0, None)

    # Map the length of the numeric prefix to hierarchy level
    level_map = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}

    level = level_map.get(len(numeric_prefix), 0)
    return (level, numeric_prefix if level > 0 else None)


def filter_object_item_fields(item_dict: dict, object_type: Type[BaseModel]) -> dict:
    """Keep only fields that belong to TextItem model."""
    return {
        k: v for k, v in item_dict.items() if k in set(object_type.model_fields.keys())
    }


def make_chunk_dict(chunk):

    pages = [[x.page_no for x in x.prov] for x in chunk.meta.doc_items]
    unique_pages = list(set([page for sublist in pages for page in sublist]))
    doc_refs = [x.self_ref for x in chunk.meta.doc_items]
    headings = chunk.meta.headings

    if headings:
        h1 = headings[0] if len(headings) > 0 else None
        h2 = headings[1] if len(headings) > 1 else None
        h3 = headings[2] if len(headings) > 2 else None
        h4 = headings[3] if len(headings) > 3 else None
        h5 = headings[4] if len(headings) > 4 else None
    else:
        h1 = None
        h2 = None
        h3 = None
        h4 = None
        h5 = None

    return {
        "filename": chunk.meta.origin.filename,
        "pages": unique_pages,
        "doc_refs": doc_refs,
        "text": chunk.text,
        "h1": h1,
        "h2": h2,
        "h3": h3,
        "h4": h4,
        "h5": h5,
    }


def clean_up_hierarchy(document: DoclingDocument) -> DoclingDocument:
    for item in document.texts:
        print(item.text)
        print(item.label)
        level, prefix = get_hierarchy_level(item.text)
        print(level, prefix)
        item_dict = item.model_dump()

        # Rename text that should be sections with levels
        if level > 0:
            print("renaming text section")
            item_dict = filter_object_item_fields(item_dict, SectionHeaderItem)
            item_dict.update({"label": DocItemLabel.SECTION_HEADER})
            item = SectionHeaderItem(**item_dict)
            item.level = level
        elif item.label in [
            DocItemLabel.SECTION_HEADER,
            DocItemLabel.TITLE,
            DocItemLabel.DOCUMENT_INDEX,
            DocItemLabel.PARAGRAPH,
        ]:
            print("renaming section or title")
            # Rename sections and titles that should actually be text for chunking
            item_dict = filter_object_item_fields(item_dict, TextItem)
            item_dict.update({"label": DocItemLabel.TEXT})
            item = TextItem(**item_dict)
        else:
            print("keep as is")
            item = item

    return document
