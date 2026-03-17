import re

def _apply_adf_marks(text, marks):
    if not text:
        return ''
    href = None
    for mark in marks or []:
        mark_type = mark.get('type')
        if mark_type == 'strong':
            text = f"**{text}**"
        elif mark_type in ['em', 'italic']:
            text = f"*{text}*"
        elif mark_type == 'code':
            text = f"`{text}`"
        elif mark_type == 'link':
            href = (mark.get('attrs') or {}).get('href')
    if href:
        text = f"[{text}]({href})"
    return text

def _jira_adf_to_text(node):
    if isinstance(node, dict):
        node_type = node.get('type')
        if node_type == 'text':
            text = node.get('text', '')
            marks = node.get('marks') or []
            return _apply_adf_marks(text, marks)
        if node_type == 'mention':
            attrs = node.get('attrs') or {}
            text = attrs.get('text') or attrs.get('displayName') or attrs.get('id') or ''
            if not text:
                return ''
            return text if text.startswith('@') else f"@{text}"
        if node_type == 'hardBreak':
            return '\n'
        content = node.get('content') or []
        if node_type == 'paragraph':
            text = ''.join(_jira_adf_to_text(item) for item in content)
            return text + '\n' if text else '\n'
        if node_type == 'heading':
            text = ''.join(_jira_adf_to_text(item) for item in content).strip()
            level = (node.get('attrs') or {}).get('level') or 1
            level = max(1, min(6, int(level)))
            return f"{'#' * level} {text}\n"
        if node_type == 'blockquote':
            text = ''.join(_jira_adf_to_text(item) for item in content)
            lines = [f"> {line}" if line else ">" for line in text.splitlines()]
            return '\n'.join(lines) + '\n'
        if node_type in ['bulletList', 'orderedList']:
            items = []
            for index, item in enumerate(content, start=1):
                item_text = _jira_adf_to_text(item).strip()
                if not item_text:
                    continue
                prefix = '- ' if node_type == 'bulletList' else f"{index}. "
                items.append(prefix + item_text)
            return '\n'.join(items) + '\n'
        if node_type == 'listItem':
            return ''.join(_jira_adf_to_text(item) for item in content)
        return ''.join(_jira_adf_to_text(item) for item in content)
    if isinstance(node, list):
        return ''.join(_jira_adf_to_text(item) for item in node)
    if isinstance(node, str):
        return node
    return ''

def normalize_jira_text(value):
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        text = _jira_adf_to_text(value)
    else:
        text = str(value)
    text = text.replace('\r\n', '\n')
    for level in range(6, 0, -1):
        text = re.sub(rf'(?m)^h{level}\.\s+', '#' * level + ' ', text)
    text = re.sub(r'\[~accountid:([^\]]+)\]', r'@\1', text)
    return text.strip()
