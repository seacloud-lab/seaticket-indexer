import re
from html2text import HTML2Text

from pptx import Presentation
from operator import attrgetter
from pptx.enum.shapes import PP_PLACEHOLDER, MSO_SHAPE_TYPE

import xml.etree.ElementTree as ET
from docx import Document
from docx.opc.pkgreader import _SerializedRelationships, _SerializedRelationship
from docx.opc.oxml import parse_xml
from docx.document import Document as _Document
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph
from docx.text.run import Run
from docx.text.hyperlink import Hyperlink
from docx.enum.text import WD_ALIGN_PARAGRAPH
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer, LTChar, LTRect, LTLine

from typing import Any, Dict, List, Optional


md_hander = HTML2Text(bodywidth=0)  # no wrapping length

HEADER_LABEL = [
    'header1',
    'header2',
    'header3',
    'header4',
    'header5',
    'header6',
]

class _NotionMarkdownRenderer:
    def __init__(self, indent_size=2):
        self.indent_size = indent_size
        self._heading_levels = {'heading_1': 1, 'heading_2': 2, 'heading_3': 3}
        self._renderers = {
            'column_list': self._render_container,
            'column': self._render_container,
            'paragraph': self._render_paragraph,
            'heading_1': self._render_heading,
            'heading_2': self._render_heading,
            'heading_3': self._render_heading,
            'bulleted_list_item': self._render_bulleted_list_item,
            'numbered_list_item': self._render_numbered_list_item,
            'to_do': self._render_to_do,
            'toggle': self._render_toggle,
            'quote': self._render_quote,
            'code': self._render_code,
            'divider': self._render_divider,
            'callout': self._render_callout,
            'image': self._render_image,
            'bookmark': self._render_url_block,
            'embed': self._render_url_block,
            'link_preview': self._render_url_block,
            'file': self._render_media_link,
            'pdf': self._render_media_link,
            'video': self._render_media_link,
            'audio': self._render_media_link,
            'child_page': self._render_child_page,
            'table': self._render_table,
        }

    def _escape_md(self, text):
        if not text:
            return ''
        return text.replace('\\', '\\\\')

    def _apply_annotations(self, text, annotations):
        if text is None:
            return ''
        if annotations.get('code'):
            text = f"`{text}`"
        if annotations.get('bold'):
            text = f"**{text}**"
        if annotations.get('italic'):
            text = f"_{text}_"
        if annotations.get('strikethrough'):
            text = f"~~{text}~~"
        return text

    def _rich_text_to_md(self, rich_text):
        if not rich_text:
            return ''
        parts = []
        for rt in rich_text:
            plain = rt.get('plain_text')
            if plain is None:
                if rt.get('type') == 'text':
                    plain = (rt.get('text') or {}).get('content', '')
                else:
                    plain = ''
            text = self._escape_md(plain)
            text = self._apply_annotations(text, rt.get('annotations') or {})
            href = rt.get('href')
            if not href and rt.get('type') == 'text':
                href = (rt.get('text') or {}).get('link', {}) or None
                if isinstance(href, dict):
                    href = href.get('url')
            if href:
                text = f"[{text}]({href})"
            parts.append(text)
        return ''.join(parts)

    def _get_block_rich_text(self, block):
        block_type = block.get('type')
        if not block_type:
            return []
        data = block.get(block_type)
        if not isinstance(data, dict):
            return []
        rich_text = data.get('rich_text')
        if isinstance(rich_text, list):
            return rich_text
        return []

    def _get_block_text(self, block):
        return self._rich_text_to_md(self._get_block_rich_text(block))

    def _render_line_with_children(self, line, children, indent, child_indent=None):
        if not children:
            return [line] if line else []
        rendered_children = self.render_blocks(
            children,
            indent=indent + 1 if child_indent is None else child_indent,
        )
        if line:
            return [line] + rendered_children
        return rendered_children

    def _prefix_lines(self, lines, prefix):
        return [prefix + line if line.strip() else line for line in lines]

    def _render_blockquote_lines(self, text, indent):
        prefix = self._prefix(indent) + '> '
        base_lines = [l for l in (text.split('\n') if text else ['']) if l.strip()]
        return self._prefix_lines(base_lines, prefix)

    def _extract_file_url(self, file_obj):
        if not file_obj:
            return ''
        if file_obj.get('type') == 'external':
            return (file_obj.get('external') or {}).get('url', '')
        if file_obj.get('type') == 'file':
            return (file_obj.get('file') or {}).get('url', '')
        return file_obj.get('url', '')

    def _render_table_block(self, block, indent):
        table = block.get('table') or {}
        children = block.get('children') or []
        has_row_header = bool(table.get('has_row_header'))

        rows = [
            [
                self._rich_text_to_md(cell).replace('\n', ' ')
                for cell in (row.get('table_row') or {}).get('cells') or []
            ]
            for row in children
            if row.get('type') == 'table_row'
        ]
        if not rows:
            return []

        max_cols = max((len(r) for r in rows), default=0)
        if max_cols == 0:
            return []

        def pad_row(r):
            return [(r[i] if i < len(r) and r[i] else ' ') for i in range(max_cols)]

        rows = [pad_row(r) for r in rows]

        header_row = rows[0]
        start_idx = 1

        prefix = self._prefix(indent)
        lines = [
            prefix + '| ' + ' | '.join(header_row) + ' |',
            prefix + '| ' + ' | '.join(['---'] * max_cols) + ' |',
        ]

        for r in rows[start_idx:]:
            if has_row_header and r:
                r = [f"**{r[0]}**"] + r[1:]
            lines.append(prefix + '| ' + ' | '.join(r) + ' |')
        return lines

    def render_page(self, full_page, include_title=True):
        title = (full_page or {}).get('title') or ''
        blocks = (full_page or {}).get('blocks') or []

        out_lines = []
        if include_title and title:
            out_lines.append('# ' + self._escape_md(title))
            out_lines.append('')

        out_lines.extend(self.render_blocks(blocks, indent=0))
        return '\n'.join(out_lines).strip() + '\n'

    def render_blocks(self, blocks, indent=0):
        lines = []
        lines_append = lines.append
        lines_extend = lines.extend
        i = 0
        while i < len(blocks):
            block = blocks[i]
            t = block.get('type')

            if t in ('numbered_list_item', 'bulleted_list_item'):
                group_lines, i = self._render_list_group(blocks, indent, i, t)
                lines_extend(group_lines)
                continue

            lines_extend(self.render_block(block, indent=indent))
            lines_append('')
            i += 1

        while lines and lines[-1] == '':
            lines.pop()
        return lines

    def render_block(self, block, indent, list_number=None):
        block_type = block.get('type')
        if not block_type:
            return []

        renderer = self._renderers.get(block_type, self._render_default)
        if block_type == 'numbered_list_item':
            return renderer(block, indent, list_number)
        return renderer(block, indent)

    def _prefix(self, indent):
        return ' ' * (indent * self.indent_size)

    def _render_container(self, block, indent):
        children = block.get('children') or []
        if not children:
            return []
        return self.render_blocks(children, indent=indent)

    def _render_paragraph(self, block, indent):
        text = self._get_block_text(block)
        line = self._prefix(indent) + text if text else ''
        return self._render_line_with_children(line, block.get('children') or [], indent, child_indent=indent)

    def _render_heading(self, block, indent):
        block_type = block.get('type')
        level = self._heading_levels[block_type]
        text = self._get_block_text(block)
        return [self._prefix(indent) + ('#' * level) + ' ' + text]

    def _render_bulleted_list_item(self, block, indent):
        text = self._get_block_text(block)
        line = self._prefix(indent) + '- ' + text
        return self._render_line_with_children(line, block.get('children') or [], indent)

    def _render_numbered_list_item(self, block, indent, list_number):
        text = self._get_block_text(block)
        number = list_number if list_number is not None else 1
        line = self._prefix(indent) + f"{number}. " + text
        return self._render_line_with_children(line, block.get('children') or [], indent)

    def _render_to_do(self, block, indent):
        todo = block.get('to_do') or {}
        text = self._get_block_text(block)
        checked = bool(todo.get('checked'))
        mark = 'x' if checked else ' '
        line = self._prefix(indent) + f"- [{mark}] " + text
        return self._render_line_with_children(line, block.get('children') or [], indent)

    def _render_toggle(self, block, indent):
        text = self._get_block_text(block)
        line = self._prefix(indent) + '<details>'
        summary = self._prefix(indent) + f"<summary>{text}</summary>"
        children = block.get('children') or []
        rendered_children = self.render_blocks(children, indent=indent + 1) if children else []
        end = self._prefix(indent) + '</details>'
        return [line, summary] + rendered_children + [end]

    def _render_quote(self, block, indent):
        text = self._get_block_text(block)
        prefix = self._prefix(indent)
        lines = self._render_blockquote_lines(text, indent)
        children = block.get('children') or []
        if children:
            child_lines = self.render_blocks(children, indent=indent)
            child_lines = self._prefix_lines(child_lines, prefix + '> ')
            return lines + child_lines
        return lines

    def _render_code(self, block, indent):
        code = block.get('code') or {}
        lang = code.get('language') or ''
        text = self._get_block_text(block)
        prefix = self._prefix(indent)
        return [prefix + f"```{lang}", text, prefix + "```"]

    def _render_divider(self, block, indent):
        return [self._prefix(indent) + '---']

    def _render_callout(self, block, indent):
        callout = block.get('callout') or {}
        icon = callout.get('icon') or {}
        icon_text = ''
        if icon.get('type') == 'emoji':
            icon_text = icon.get('emoji', '')
        text = self._get_block_text(block)
        head = (icon_text + ' ' if icon_text else '') + text
        prefix = self._prefix(indent)
        line = prefix + '> ' + head if head else (prefix + '>')
        children = block.get('children') or []
        rendered_children = self.render_blocks(children, indent=indent) if children else []
        rendered_children = self._prefix_lines(rendered_children, prefix + '> ')
        return [line] + rendered_children

    def _render_image(self, block, indent):
        img = block.get('image') or {}
        url = self._extract_file_url(img)
        caption = self._rich_text_to_md(img.get('caption'))
        alt = caption or 'image'
        if url:
            return [self._prefix(indent) + f"![{alt}]({url})"]
        return []

    def _render_url_block(self, block, indent):
        block_type = block.get('type')
        data = block.get(block_type)
        if not isinstance(data, dict):
            return []
        url = data.get('url') or ''
        if url:
            return [self._prefix(indent) + url]
        return []

    def _render_media_link(self, block, indent):
        block_type = block.get('type')
        data = block.get(block_type)
        if not isinstance(data, dict):
            return []
        url = self._extract_file_url(data)
        caption = self._rich_text_to_md(data.get('caption'))
        label = caption or block_type
        if url:
            return [self._prefix(indent) + f"[{label}]({url})"]
        return []

    def _render_child_page(self, block, indent):
        title = (block.get('child_page') or {}).get('title', '')
        if title:
            return [self._prefix(indent) + '## ' + self._escape_md(title)]
        return []

    def _render_table(self, block, indent):
        return self._render_table_block(block, indent)

    def _render_default(self, block, indent):
        text = self._rich_text_to_md(self._get_block_rich_text(block))
        if text:
            return [self._prefix(indent) + text]
        return []

    def _render_list_group(self, blocks, indent, start_index, block_type):
        lines = []
        i = start_index
        if block_type == 'numbered_list_item':
            n = 1
            while i < len(blocks) and blocks[i].get('type') == block_type:
                lines.extend(self.render_block(blocks[i], indent=indent, list_number=n))
                n += 1
                i += 1
        else:
            while i < len(blocks) and blocks[i].get('type') == block_type:
                lines.extend(self.render_block(blocks[i], indent=indent))
                i += 1
        lines.append('')
        return lines, i


_NOTION_RENDERER = _NotionMarkdownRenderer()


def notion_page_to_markdown(full_page, include_title=True):
    return _NOTION_RENDERER.render_page(full_page, include_title=include_title)


def _handle_text_style(json_data_text, return_null=False):
    text = json_data_text.get('text', '')
    pure_text = text
    bold = json_data_text.get('bold')
    italic = json_data_text.get('italic')

    if italic:
        text = "_%s_" % text
    if bold:
        text = "**%s**" % text

    if (not text) and return_null:
        text = '.'
    return text, pure_text


def _handle_header_dom(header_json, header_type):
    output = ''
    for child in header_json['children']:
        if 'text' in child:
            output += child.get('text')
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)

    tag = {
        "header1": "<h1>%s</h1>",
        "header2": "<h2>%s</h2>",
        "header3": "<h3>%s</h3>",
        "header4": "<h4>%s</h4>",
        "header5": "<h5>%s</h5>",
        "header6": "<h6>%s</h6>",

    }.get(header_type)
    return tag % output


def _handle_img_dom(img_json):
    output = ''
    url = img_json.get('data', {}).get('src')

    output += '<img src="%s">' % url
    return output


def _handle_list_dom(list_json, indent_level=0, ordered=False):
    '''
    list_json: list json data
    indent_level: indent level
    ordered: whether the list is ordered
    '''
    result = []

    for index, list_item in enumerate(list_json['children'], 1):
        item_eles = list_item['children']
        current_line = '    ' * indent_level  # Use 4 Spaces as an indent

        # add mark according to list type
        if ordered:
            current_line += f"{index}. "
        else:
            current_line += "* "

        has_added_current_line = False

        # handle current line
        for lic in item_eles:
            if lic.get('type') == 'paragraph':
                for item in lic['children']:
                    if 'text' in item:
                        current_line += _handle_text_style(item)[0]
                    else:
                        item_type = item.get('type')
                        if item_type == 'link':
                            text_name = item['children'][0]['text']
                            text_url = item.get('href')
                            current_line += f"[{text_name}]({text_url})"

                if not has_added_current_line:
                    result.append(current_line)
                    has_added_current_line = True

            # handle sub list
            elif lic.get('type') in ['ordered_list', 'unordered_list']:
                if not has_added_current_line and current_line.strip() != ('* ' if not ordered else f"{index}. "):
                    result.append(current_line)
                    has_added_current_line = True

                # recursive handle sub list
                sub_list = _handle_list_dom(
                    lic,
                    indent_level + 1,
                    ordered=(lic.get('type') == 'ordered_list')
                )
                result.extend(sub_list)

    return result


def _handle_check_list_dom(check_list_json):
    output = ""
    checked = check_list_json.get('checked')
    for child in check_list_json['children']:
        if 'text' in child:
            output += _handle_text_style(child)[0]
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)

    if checked:
        output = "<p><span>* [x] %s</span></p>" % output
    else:
        output = "<p><span>* [ ] %s</span></p>" % output

    return output


def _handle_blockquote_dom(blockquote_json):
    output = ""
    for child in blockquote_json['children']:
        child_type = child.get('type')
        if child_type in ['ordered_list', 'unordered_list']:
            output += _handle_list_dom(child, '', child_type == 'ordered_list')

        if child_type == 'link':
            text_name = child['children'][0]['text']
            text_url = child.get('href')
            output += "<a href='%s'><span>%s</span></a>" % (text_url, text_name)

        if child_type == 'paragraph':
            output += '%s' % _handle_pagragh_dom(child)

        if child_type == 'check_list_item':
            output += '%s' % _handle_check_list_dom(child)

        if child_type == 'image':
            output += _handle_img_dom(child)

        if 'text' in child:
            text = child.get('text')
            text_list = text.split("\n")
            output += ''.join(['<p><span>%s</span></p>' % t for t in text_list if t.strip()])

    tag = "<blockquote>%s</blockquote>" % output
    return tag


def _handle_link_dom(link_json):
    href = link_json.get('href')
    link_child = link_json['children'][0]

    res = "<a href='%s'><span>%s</span></a>" % (href, link_child.get('text'))
    return res


def _handle_pagragh_dom(pagragh_json):
    output = ''
    for child in pagragh_json['children']:
        if 'text' in child:
            output += _handle_text_style(child)[0]
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)
            if child_type == 'image':
                output += _handle_img_dom(child)

    result = "<p><span>%s</span></p>" % output
    return result.replace("\n", "")


def _handle_table_cell_dom(table_cell_json):
    output = ''
    for child in table_cell_json['children']:
        if 'text' in child:
            text, _ = _handle_text_style(child)
            # replace newline with space
            output += text.replace('\n', ' ')
        else:
            child_type = child.get('type')
            if child_type == 'link':
                text_name = child['children'][0]['text']
                text_url = child.get('href')
                output += f"[{text_name}]({text_url})"

    return output.strip()


def handle_header(header_json, header_type):
    dom = _handle_header_dom(header_json, header_type)
    return md_hander.handle(dom)


def handle_img(img_json):
    dom = _handle_img_dom(img_json)
    return dom


def handle_check_list(check_list_json):
    return md_hander.handle(_handle_check_list_dom(check_list_json))


def handle_paragraph(paragraph_json):
    dom = _handle_pagragh_dom(paragraph_json)
    return md_hander.handle(dom)


def handle_list(json_data, ordered=False):
    # return processed markdown text directly
    lines = _handle_list_dom(json_data, 0, ordered)
    return '\n'.join(lines)


def handle_codeblock(code_bloc_json):
    lang = code_bloc_json.get('language', '')
    output = ""
    for child in code_bloc_json.get('children'):
        if 'children' in child:
            output += "%s\n" % child.get('children', '')[0].get('text')
    return "```%s\n%s```" % (lang, output)


def handle_blockquote(json_data):
    html = _handle_blockquote_dom(json_data)
    md = md_hander.handle(html)
    return md


def handle_table(table_json):
    if not table_json['children']:
        return '\n'

    rows = []
    table_rows = table_json['children']

    # get all rows and actual column count
    max_cols = 0
    all_rows = []

    # first scan: determine actual column count and save all rows
    for row in table_rows:
        row_contents = []
        for cell in row['children']:
            content = _handle_table_cell_dom(cell).strip()
            if content.endswith('+'):
                content = content + ' '  # add space after "+"
            row_contents.append(content)
        # update max column count, no longer filter empty rows
        max_cols = max(max_cols, len(row_contents))
        all_rows.append(row_contents)

    # if column count is 0, return empty line
    if max_cols == 0:
        return '\n'

    # handle header
    header_row = all_rows[0]
    header_cells = []
    for i in range(max_cols):
        content = header_row[i] if i < len(header_row) else ' '
        header_cells.append(content or ' ')
    rows.append('| ' + ' | '.join(header_cells) + ' |')

    # add separator line
    rows.append('| ' + ' | '.join(['---'] * max_cols) + ' |')

    # handle all data rows, including empty rows
    for row_cells in all_rows[1:]:
        formatted_cells = []
        for i in range(max_cols):
            content = row_cells[i] if i < len(row_cells) else ' '
            formatted_cells.append(content or ' ')
        rows.append('| ' + ' | '.join(formatted_cells) + ' |')

    return '\n' + '\n'.join(rows) + '\n'


def handle_callout(json_data):
    children = json_data.get('children')
    callout = ''

    for child in children:
        output = handle_paragraph(child)
        callout += output
    return callout


def handle_image_block(json_data):
    children = json_data.get('children', [])

    if not children:
        return ''

    for child in children:
        child_type = child.get('type')

        if child_type == 'image':
            data = child.get('data', {})
            src = data.get('src', '')

            alt_text = ''
            if 'children' in child and child['children']:
                alt_text = child['children'][0].get('text', '')

            return f"![{alt_text}]({src})\n"

        elif 'text' in child:
            text = child.get('text', '').strip()
            if text:
                return f"{text}\n"

    return ''


def json2md(json_data):
    doc_type = json_data.get('type')
    markdown_output = ''
    if doc_type in HEADER_LABEL:
        output = handle_header(json_data, doc_type)
        markdown_output += output

    if doc_type == 'check_list_item':
        output = handle_check_list(json_data)
        markdown_output += output

    if doc_type == 'paragraph':
        output = handle_paragraph(json_data)
        markdown_output += output

    if doc_type == 'code_block':
        output = handle_codeblock(json_data)
        markdown_output += output

    if doc_type == 'table':
        output = handle_table(json_data)
        markdown_output += output

    if doc_type == 'unordered_list':
        output = handle_list(json_data)
        markdown_output += output

    if doc_type == 'ordered_list':
        output = handle_list(json_data, ordered=True)
        markdown_output += output

    if doc_type == 'blockquote':
        output = handle_blockquote(json_data)
        markdown_output += output

    if doc_type == 'callout':
        output = handle_callout(json_data)
        markdown_output += output

    if doc_type == 'image_block':
        output = handle_image_block(json_data)
        markdown_output += output

    return markdown_output


def sdoc_to_markdown(json_tree):
    results = []
    for sub in json_tree.get('elements'):
        results.append(json2md(sub))

    markdown_text = "\n".join(results)
    return markdown_text


# ******* parse pptx ******
def is_title(shape):
    if shape.is_placeholder and (
        shape.placeholder_format.type == PP_PLACEHOLDER.TITLE
        or shape.placeholder_format.type == PP_PLACEHOLDER.SUBTITLE
        or shape.placeholder_format.type == PP_PLACEHOLDER.VERTICAL_TITLE
        or shape.placeholder_format.type == PP_PLACEHOLDER.CENTER_TITLE
    ):
        return True
    return False


def is_list_block(shape):
    levels = []
    for para in shape.text_frame.paragraphs:
        if para.level not in levels:
            levels.append(para.level)
        if para.level != 0 or len(levels) > 1:
            return True
    return False


def put_list(text, level):
    return '  ' * level + '* ' + text.strip() + '\n'


def put_table(table):
    table_text = ''
    gen_table_row = (
        lambda row: '| ' + ' | '.join([c.replace('\n', '<br />') for c in row]) + ' |'
    )
    table_text += gen_table_row(table[0]) + '\n'
    table_text += gen_table_row([':-:' for _ in table[0]]) + '\n'
    table_text += '\n'.join([gen_table_row(row) for row in table[1:]]) + '\n\n'
    return table_text


def process_text_block(shape):
    text = ''
    if is_list_block(shape):
        # generate list block
        for para in shape.text_frame.paragraphs:
            if para.text.strip() == '':
                continue
            text += put_list(para.text, para.level)
    else:
        # generate paragraph block
        for para in shape.text_frame.paragraphs:
            if para.text.strip() == '':
                continue
            text += para.text
    return text


def process_table(shape, _):
    table = [[cell.text for cell in row.cells] for row in shape.table.rows]
    if len(table) > 0:
        return put_table(table)
    else:
        return ''


def process_shapes(current_shapes, slide_id):
    slide_text = ''
    for idx, shape in enumerate(current_shapes):
        if is_title(shape):
            slide_text += shape.text_frame.text.strip() + '\n'
        elif shape.shape_type == MSO_SHAPE_TYPE.TABLE:
            slide_text += process_table(shape, slide_id + 1) + '\n'
        elif shape.has_text_frame:
            slide_text += process_text_block(shape) + '\n'
    return slide_text + '-' * 25 + '\n'


def ungroup_shapes(shapes):
    res = []
    for shape in shapes:
        if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
            res.extend(ungroup_shapes(shape.shapes))
        else:
            res.append(shape)
    return res


def pptx_to_markdown(file):
    prs = Presentation(file)
    pptx_text = ''
    for idx, slide in enumerate(prs.slides):
        try:
            shapes = sorted(ungroup_shapes(slide.shapes), key=attrgetter('top', 'left'))
            pptx_text += process_shapes(shapes, idx)
        except Exception:
            continue
    return pptx_text


# ****** parse pdf *******
def get_text_info(element):
    """Extract the content, average font size, and left indentation of text blocks"""
    text = element.get_text().strip()
    if not text:
        return None, None, None

    # Calculate the average font size (taking the average of all characters)
    chars = [c for c in element if isinstance(c, LTChar)]
    if not chars:
        return text, None, element.x0  # x0 is the left indentation coordinate
    avg_font_size = sum(c.size for c in chars) / len(chars)
    return text, round(avg_font_size, 1), element.x0


def is_list_item(text, x0, prev_x0, list_indent_threshold=20):
    """Determine whether it is a list item by indentation and prefix symbols"""
    # List items usually have indentation (x0 greater than regular paragraphs)
    if x0 - prev_x0 > list_indent_threshold:
        return True
    # Contains list symbols (such as 1., -, •, etc.)
    list_pattern = re.compile(r'^(\d+\.|\*|-|•)\s+')
    if list_pattern.match(text):
        return True
    return False


def table_to_markdown(table_data):
    """Convert table data to Markdown format"""
    if not table_data:
        return ""
    max_cols = max(len(row) for row in table_data)
    # Fill in the number of columns
    table_data = [row + [""] * (max_cols - len(row)) for row in table_data]
    md_table = []
    # header
    md_table.append("| " + " | ".join(table_data[0]) + " |")
    # dividing line
    md_table.append("| " + " | ".join(["---"] * max_cols) + " |")
    # Content line
    for row in table_data[1:]:
        md_table.append("| " + " | ".join(row) + " |")
    return "\n".join(md_table)


def extract_table_from_page(page):
    """Extract table data from the page (grouped based on text coordinates)"""
    texts = []
    for element in page:
        if isinstance(element, LTTextContainer):
            text, _, x0 = get_text_info(element)
            if text:
                # Storage (y0: vertical coordinate, x0: horizontal coordinate, text: content)
                texts.append((-element.y0, x0, text))  # Negative sign is used for sorting (the larger y0, the higher it is)

    if len(texts) < 2:
        return None  # Less than 2 rows are not considered as a table

    # Sort by y0 (top to bottom) and group into rows (same row for y-phase myopia)
    texts.sort()
    rows = []
    current_row = [texts[0]]
    y_threshold = 5  # Y-coordinate difference threshold
    for t in texts[1:]:
        if abs(t[0] - current_row[-1][0]) < y_threshold:
            current_row.append(t)
        else:
            rows.append(sorted(current_row, key=lambda x: x[1]))  # Sort by x within the row
            current_row = [t]
    rows.append(sorted(current_row, key=lambda x: x[1]))

    # Extract each line of text
    table_data = []
    for row in rows:
        table_data.append([t[2] for t in row])
    return table_data


def pdf_to_markdown(pdf_path):
    md_content = []
    prev_x0 = 0
    in_list = False

    for page_num, page in enumerate(extract_pages(pdf_path), 1):
        table_data = extract_table_from_page(page)
        if table_data:
            md_content.append(table_to_markdown(table_data) + "\n")

        # Extract text content (exclude text from the table to avoid duplication)
        for element in page:
            if isinstance(element, LTTextContainer) and not isinstance(element, (LTRect, LTLine)):
                text, font_size, x0 = get_text_info(element)
                if not text:
                    continue

                # Identify the title (assuming font size corresponds to hierarchy)
                if font_size and font_size > 16:
                    md_content.append(f"# {text}\n")
                    in_list = False
                elif font_size and font_size > 14:
                    md_content.append(f"## {text}\n")
                    in_list = False
                elif font_size and font_size > 12:
                    md_content.append(f"### {text}\n")
                    in_list = False
                else:
                    # Identify list items
                    if is_list_item(text, x0, prev_x0):
                        # Remove the prefix of the list symbol (such as "1 "," - ")
                        text = re.sub(r'^(\d+\.|\*|-|•)\s+', '', text)
                        # Calculate indentation level (every 20 pixels is one level)
                        indent = int((x0 - prev_x0) // 20) * 2
                        md_content.append(f"{' ' * indent}- {text}\n")
                        in_list = True
                    else:
                        # Ordinary paragraph
                        if in_list:
                            md_content.append("\n")  # Add a blank line after the end of the list
                            in_list = False
                        md_content.append(f"{text}\n")

                prev_x0 = x0 if not in_list else prev_x0  # List items do not update baseline indentation

    return "\n".join(md_content)


# ****** parse docx *******

def load_from_xml_v2(baseURI, rels_item_xml):
    """
    Return |_SerializedRelationships| instance loaded with the
    relationships contained in *rels_item_xml*. Returns an empty
    collection if *rels_item_xml* is |None|.
    """
    srels = _SerializedRelationships()
    if rels_item_xml is not None:
        rels_elm = parse_xml(rels_item_xml)
        for rel_elm in rels_elm.Relationship_lst:
            if rel_elm.target_ref in ('../NULL', 'NULL'):
                continue
            srels._srels.append(_SerializedRelationship(baseURI, rel_elm))
    return srels


def iter_block_items(parent):
    """
    Generate a reference to each paragraph and table child within *parent*,
    in document order. Each returned value is an instance of either Table or
    Paragraph. *parent* would most commonly be a reference to a main
    Document object, but also works for a _Cell object, which itself can
    contain paragraphs and tables.
    """
    if isinstance(parent, _Document):
        parent_elm = parent.element.body
    elif isinstance(parent, _Cell):
        parent_elm = parent._tc

    for child in parent_elm.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, parent)
        elif isinstance(child, CT_Tbl):
            yield Table(child, parent)


def parse_block_contents(items):
    md_content = []
    for item in items:
        if isinstance(item, Run):
            if text := item.text:
                md_content.append(text)

        elif isinstance(item, Hyperlink):
            hyperlink_text = f'[{item.text}]({item.url})'
            md_content.append(hyperlink_text)

    return ''.join(md_content)


def parse_heading(block, header_level):
    md_content = parse_block_contents(block.iter_inner_content())
    return f"{'#' * int(header_level)} {md_content}"


def parse_paragraph(block):
    md_content = parse_block_contents(block.iter_inner_content())
    return md_content


def _get_numFmt(numbering_xml, num_id, ilvlid):
    root = ET.fromstring(numbering_xml)
    namespace = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}

    # Find the <w:num> element with the given numId
    num_element = root.find(f".//w:num[@w:numId='{num_id}']", namespace)
    if num_element is None:
        return None

    # Get the abstractNumId value from the <w:num> element
    abstract_num_id = num_element.find("w:abstractNumId", namespace).get("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}val")

    # Find the corresponding <w:abstractNum> element with the abstractNumId
    abstract_num_element = root.find(f".//w:abstractNum[@w:abstractNumId='{abstract_num_id}']", namespace)
    if abstract_num_element is None:
        return None

    # Find the <w:lvl> element with the given ilvlid
    lvl_element = abstract_num_element.find(f".//w:lvl[@w:ilvl='{ilvlid}']", namespace)
    if lvl_element is None:
        return None

    # Get the numFmt value from the <w:lvl> element
    num_fmt = lvl_element.find("w:numFmt", namespace)
    if num_fmt is not None:
        return num_fmt.get("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}val")
    else:
        return None


def parse_list(block, numbering_xml):
    if block._element.pPr.numPr is not None:
        num_id = block._element.pPr.numPr.numId.val
    elif block.style._element.pPr.numPr is not None:
        num_id = block.style._element.pPr.numPr.numId.val
    else:
        return parse_paragraph(block)
    if block._element.pPr.numPr is not None and block._element.pPr.numPr.ilvl is not None:
        ilvl_id = block._element.pPr.numPr.ilvl.val
    elif block.style._element.pPr.numPr is not None and block.style._element.pPr.numPr.ilvl is not None:
        ilvl_id = block.style._element.pPr.numPr.ilvl.val
    else:
        return parse_paragraph(block)

    num_fmt = _get_numFmt(numbering_xml, num_id, ilvl_id)
    content = parse_block_contents(block.iter_inner_content())
    if num_fmt in {'decimal', 'lowerLetter', 'lowerRoman', 'upperLetter', 'upperRoman'}:
        # ordered_list
        return f"{str(num_id)}. {content}"
    else:
        return f"- {content}"


def parse_table(table):
    md_content = []
    header = [cell.text for cell in table.rows[0].cells]
    md_content.append("| " + " | ".join(header) + " |")
    md_content.append("| " + " | ".join(["---"] * len(header)) + " |")
    for row in table.rows[1:]:
        md_content.append("| " + " | ".join([cell.text for cell in row.cells]) + " |")

    return "\n".join(md_content)


def parse_alignment(block):
    if not hasattr(block, 'alignment'):
        return None
    if block.alignment == WD_ALIGN_PARAGRAPH.LEFT:
        return {'align': 'left'}
    elif block.alignment == WD_ALIGN_PARAGRAPH.CENTER:
        return {'align': 'center'}
    elif block.alignment == WD_ALIGN_PARAGRAPH.RIGHT:
        return {'align': 'right'}
    else:
        return None


def docx_to_markdown(content):
    content_list = []
    # Fix according to: https://github.com/python-openxml/python-docx/issues/1105s
    _SerializedRelationships.load_from_xml = load_from_xml_v2

    docx = Document(content)
    try:
        numbering_xml = docx.part.numbering_part.element.xml
    except (KeyError, NotImplementedError):
        numbering_xml = None
    header_styles_map = {
        'Title': 'header1',
        'Subtitle': 'header1',
        'Heading 1': 'header1',
        'Heading 2': 'header2',
        'Heading 3': 'header3',
        'Heading 4': 'header4',
        'Heading 5': 'header5',
        'Heading 6': 'header6',
        'Heading 7': 'header6',
        'Heading 8': 'header6',
        'Heading 9': 'header6',
    }
    for block in iter_block_items(docx):
        style = block.style
        style_name = block.style.name
        base_style_name = style.base_style.name if style.base_style else ''
        is_normal_table = style_name == 'Normal Table' or base_style_name == 'Normal Table'

        if style_name in header_styles_map:
            md_content = parse_heading(block, header_styles_map[style_name][-1])
        elif style_name == 'Normal':
            md_content = parse_paragraph(block)
        elif style_name.startswith('List'):
            md_content = parse_list(block, numbering_xml)
        elif is_normal_table:
            is_paragraph = isinstance(block, Paragraph)
            if is_paragraph:
                md_content = parse_paragraph(block)
            else:
                md_content = parse_table(block)
        else:
            md_content = parse_paragraph(block)

        content_list.append(md_content)
    return "\n".join(content_list)
