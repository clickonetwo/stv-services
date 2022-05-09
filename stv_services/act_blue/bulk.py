#  MIT License
#
#  Copyright (c) 2022 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
import json

from .metadata import import_metadata_from_webhooks


def import_donation_metadata(filepath: str, verbose: bool = True):
    if verbose:
        print(f"Importing ActBlue webhooks from '{filepath}'...")
    total, imported, batch = 0, 0, []
    with open(filepath) as file:
        while line := file.readline():
            total += 1
            batch.append(json.loads(line.strip()))
            if total % 100 == 0:
                imported += import_metadata_from_webhooks(batch)
                if verbose:
                    print(f"Processed {total}, kept {imported}...")
        else:
            if batch:
                imported += import_metadata_from_webhooks(batch)
    if verbose:
        print(f"Imported {imported} metadata records from {total} webhooks.")
