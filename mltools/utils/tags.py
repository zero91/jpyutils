from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

def iob1_to_iob2(tags):
    """Convert tags in IOB1 format to IOB2 format inplace.

    Parameters
    ---------
    tags: list
        List of tags.

    Returns
    -------
    valid: boolean
        True if the tags is valid, otherwise False.

    """
    for i, tag in enumerate(tags):
        if tag == 'O':
            continue
        fields = tag.split('-')
        if len(fields) != 2 or fields[0] not in ['I', 'B']:
            return False

        if fields[0] == 'B':
            continue

        # convert IOB1 to IOB2
        if i == 0 or tags[i - 1] == 'O' or tags[i - 1][1:] != tag[1:]:
            tags[i] = 'B' + tag[1:]
    return True


def iob_iobes(tags):
    #TODO
    """
    IOB -> IOBES
    """
    new_tags = []
    for i, tag in enumerate(tags):
        if tag == 'O':
            new_tags.append(tag)
        elif tag.split('-')[0] == 'B':
            if i + 1 != len(tags) and \
               tags[i + 1].split('-')[0] == 'I':
                new_tags.append(tag)
            else:
                new_tags.append(tag.replace('B-', 'S-'))
        elif tag.split('-')[0] == 'I':
            if i + 1 < len(tags) and \
                    tags[i + 1].split('-')[0] == 'I':
                new_tags.append(tag)
            else:
                new_tags.append(tag.replace('I-', 'E-'))
        else:
            raise Exception('Invalid IOB format!')
    return new_tags


def iobes_iob(tags):
    #TODO
    """
    IOBES -> IOB
    """
    new_tags = []
    for i, tag in enumerate(tags):
        if tag.split('-')[0] == 'B':
            new_tags.append(tag)
        elif tag.split('-')[0] == 'I':
            new_tags.append(tag)
        elif tag.split('-')[0] == 'S':
            new_tags.append(tag.replace('S-', 'B-'))
        elif tag.split('-')[0] == 'E':
            new_tags.append(tag.replace('E-', 'I-'))
        elif tag.split('-')[0] == 'O':
            new_tags.append(tag)
        else:
            raise Exception('Invalid format!')
    return new_tags

