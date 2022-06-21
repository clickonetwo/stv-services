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
from stv_services.airtable import bulk
from stv_services.mobilize import event, attendance


def import_and_update_all(verbose: bool = True, force: bool = False):
    """Get all new and updated events and attendances from Mobilize"""
    # events and timeslots first, because we only import attendances for those
    event.import_events(verbose, force)
    event.compute_event_status(verbose, force)
    # next, make sure the organizers are contacts and re-update the events
    # that were waiting for them to become contacts
    bulk.update_contact_records(verbose)
    bulk.update_event_records(verbose)
    # now load the attendances
    attendance.import_attendances(verbose, force)
    attendance.compute_attendance_status(verbose, force)
    # finally, force remake the calendar
    event.make_event_calendar(verbose, True)
