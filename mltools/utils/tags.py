from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import copy

def is_valid_iob(tags, version='IOB2'):
    """Judge whether `tags' is a valid IOB string.

    Parameters
    ----------
    tags: list
        List of tags.

    version: str
        The IOB version of the tags, should be one of ["IOB1", "IOB2"].

    Returns
    -------
    is_valid: boolean
        True if `tags' is in valid IOB format, otherwise False.

    """
    for i, tag in enumerate(tags):
        if tag == 'O':
            continue
        fields = tag.split('-')
        if len(fields) != 2 or fields[0] not in ['B', 'I']:
            return False

        pos, tag_name = fields
        if i == 0 or tags[i - 1].split('-') not in [['I', tag_name], ['B', tag_name]]:
            if version == 'IOB1' and pos == 'B':
                return False

            if version == 'IOB2' and pos == 'I':
                return False
    return True


def is_valid_iobes(tags):
    """Judge whether `tags' is a valid IOBES string.

    Parameters
    ----------
    tags: list
        List of tags.

    Returns
    -------
    is_valid: boolean
        True if `tags' is in valid IOBES format, otherwise False.

    """
    for i, tag in enumerate(tags):
        if tag == 'O':
            continue
        fields = tag.split('-')
        if len(fields) != 2 or fields[0] not in ['B', 'I', 'E', 'S']:
            return False

        pos, tag_name = fields
        if pos in ['B', 'I'] and (i + 1 == len(tags) \
                or tags[i + 1].split('-') not in [['I', tag_name], ['E', tag_name]]):
            return False

        if pos in ['I', 'E'] and \
                (i == 0 or tags[i - 1].split('-') not in [['B', tag_name], ['I', tag_name]]):
            return False
    return True


def iob1_to_iob2(tags):
    """Convert tags in IOB1 format to IOB2 format.

    Parameters
    ---------
    tags: list
        List of tags.

    Returns
    -------
    new_tags: list
        The new IOB2 format tags, return empty list if `tags' is not valid.

    Raises
    ------
    ValueError: If the format of input tags is not valid.

    """
    if not is_valid_iob(tags, version="IOB1"):
        raise ValueError("Invalid IOB1 format tags: '%s'" % (tags))

    new_tags = copy.deepcopy(tags)
    for i, tag in enumerate(new_tags):
        if tag == 'O':
            continue
        pos, tag_name = tag.split('-')
        if pos == 'B':
            continue

        if i == 0 or new_tags[i - 1] == 'O' or new_tags[i - 1].split('-')[1] != tag_name:
            new_tags[i] = 'B-' + tag_name
    return new_tags


def iob_to_iobes(tags, version="IOB2"):
    """Convert tags in IOB format to IOBES format.

    Parameters
    ----------
    tags: list
        List of tags.

    version: str
        The IOB version of the tags, should be one of ["IOB1", "IOB2"].

    Returns
    -------
    new_tags: list
        The new IOBES format tags, return empty list if `tags' is not valid.

    Raises
    ------
    ValueError: If the format of input tags is not valid.

    """
    if version == "IOB1":
        new_tags = iob1_to_iob2(tags)

    elif is_valid_iob(tags, version="IOB2"):
        new_tags = copy.deepcopy(tags)

    else:
        raise ValueError("Invalid IOB2 format tags: '%s'" % (tags))

    for i, tag in enumerate(new_tags):
        if tag == 'O':
            continue

        pos, tag_name = tag.split('-')
        if i + 1 == len(new_tags) or new_tags[i + 1].split('-')[0] != 'I':
            if pos == 'B':
                new_tags[i] = 'S-' + tag_name

            else: # pos == 'I':
                new_tags[i] = 'E-' + tag_name
    return new_tags


def iobes_to_iob(tags):
    """Convert tags in IOBES format to IOB2 format.

    Parameters
    ----------
    tags: list
        List of tags.

    Returns
    -------
    new_tags: list
        The new IOB2 format tags, return empty list if `tags' is not valid.

    Raises
    ------
    ValueError: If the format of input tags is not valid.

    """
    if not is_valid_iobes(tags):
        raise ValueError("Invalid tags: '%s'" % (tags))

    new_tags = copy.deepcopy(tags)
    for i, tag in enumerate(new_tags):
        if tag == 'O':
            continue
        pos, tag_name = tag.split('-')
        if pos == 'S': new_tags[i] = 'B-' + tag_name
        if pos == 'E': new_tags[i] = 'I-' + tag_name
    return new_tags

