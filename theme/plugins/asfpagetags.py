#!/usr/bin/python -B
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# asfpagetags.py -- Pelican plugin to process page tags
#

import sys
import functools
import traceback
from collections import defaultdict

from pelican import signals
from pelican.contents import Page

# Wrap funtion to catch exception
def catch_exception(func):

    @functools.wraps(func)
    def call_func(self, *args, **kw):
        try:
            func(self, *args, **kw)
        except Exception:
            print('-----', file=sys.stderr)
            traceback.print_exc()
            # exceptions here stop the build
            raise

    return call_func

@catch_exception
def page_generator_finalized(page_generator):
    """ Generate tag pages

    Scan all pages; if a page has tags, update a dict to associate
    the tag name with the page.

    Then generate a summary page (pagetags.html) listing all the tag names
    Also generate a page for each tag name listing the pages which have the tag

    """
    print(">>page_generator_finalized")
    pagetags = defaultdict(list)
    baseReader = None
    for page in page_generator.pages:
        if hasattr(page, 'tags'):
            tags = page.tags
            if isinstance(tags, str): # gfm does not generate list of Tags (yet)
                if baseReader is None:
                    from pelican.readers import BaseReader
                    baseReader = BaseReader(None)
                tags = baseReader.process_metadata("tags", tags)
            for tag in tags:
                tagn=tag.name
                pagetags[tagn].append(page)

    newPage = Page('', metadata={
        "title": "List of page tags",
        "save_as": "pagetags.html",
        "template": 'pagetags',
        "pagetags": pagetags,
        },
        source_path = "pagetags.html", # needed by asfgenid
        )

    page_generator.pages.insert(0, newPage)

    for tagn, pages in pagetags.items():
        addPage(page_generator, tagn, pages)

    print(f"<<page_generator_finalized, found {len(pagetags)} tags in {len(page_generator.pages)} pages")

def addPage(pageGenerator, tagn, pages):
    # settings = pageGenerator.settings

    # :param content: the string to parse, containing the original content.
    # :param metadata: the metadata associated to this page (optional).
    # :param settings: the settings dictionary (optional).
    # :param source_path: The location of the source of this content (if any).
    # :param context: The shared context between generators.

    newPage = Page('', metadata={
        "title": tagn,
        "save_as": f"_{tagn}.html", # use prefix to reduce chance of clash with normal pages
        "template": 'pagetag',
        "taggedpages": pages,
        },
        source_path = f"{tagn}.html", # needed by asfgenid
    )

    pageGenerator.pages.insert(0, newPage)


def register():
    signals.page_generator_finalized.connect(page_generator_finalized)
