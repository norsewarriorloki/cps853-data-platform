"""Plain-text tables for terminal recordings and LaTeX evidence listings."""

import textwrap


def visible(value):
    """Keep nulls, blank strings, and control characters distinguishable."""
    if value is None:
        return "<NULL>"
    if isinstance(value, (list, tuple)):
        return "; ".join(visible(item) for item in value) or "(none)"
    if isinstance(value, str):
        if not value:
            return "<EMPTY>"
        if value.isspace():
            return repr(value)
    return str(value).replace("\r", r"\r").replace("\n", r"\n").replace("\t", r"\t")


def print_table(headers, rows, max_widths, prefix="[validation] "):
    """Wrap long values without dropping payloads or rejection reasons."""
    records = [[visible(value) for value in row] for row in rows]
    headers = [str(header) for header in headers]
    widths = [
        min(limit, max(len(record[index]) for record in [headers, *records]))
        for index, limit in enumerate(max_widths)
    ]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def emit(line):
        print(prefix + line, flush=True)

    def emit_row(record):
        cells = [
            textwrap.wrap(value, width=width, break_on_hyphens=False,
                          replace_whitespace=False, drop_whitespace=False) or [""]
            for value, width in zip(record, widths)
        ]
        for line_index in range(max(map(len, cells))):
            emit("| " + " | ".join(
                (cell[line_index] if line_index < len(cell) else "").ljust(width)
                for cell, width in zip(cells, widths)
            ) + " |")

    emit(border)
    emit_row(headers)
    emit(border)
    for record in records:
        emit_row(record)
        emit(border)
