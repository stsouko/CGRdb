# -*- coding: utf-8 -*-
#
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
from enum import Enum


class StructureStatus(Enum):
    RAW = 0
    HAS_ERROR = 1
    CLEAR = 2


class StructureType(Enum):
    UNDEFINED = 0
    MOLECULE = 1
    REACTION = 2


class TaskStatus(Enum):
    NEW = 0
    PREPARING = 1
    PREPARED = 2
    MODELING = 3
    DONE = 4


class ModelType(Enum):
    PREPARER = 0
    MOLECULE_MODELING = 1
    REACTION_MODELING = 2
    MOLECULE_SIMILARITY = 3
    REACTION_SIMILARITY = 4
    MOLECULE_SUBSTRUCTURE = 5
    REACTION_SUBSTRUCTURE = 6

    @staticmethod
    def select(structure_type, task_type):
        return ModelType['%s_%s' % (structure_type.name, task_type.name)]

    def compatible(self, structure_type, task_type):
        return self.name == '%s_%s' % (structure_type.name, task_type.name)


class TaskType(Enum):
    MODELING = 0
    SIMILARITY = 1
    SUBSTRUCTURE = 2


class AdditiveType(Enum):
    SOLVENT = 0
    CATALYST = 1
    OVER = 2


class ResultType(Enum):
    TEXT = 0
    STRUCTURE = 1
    TABLE = 2
    IMAGE = 3
    GRAPH = 4
    GTM = 5


class UserRole(Enum):
    COMMON = 1
    ADMIN = 2
    DATA_MANAGER = 3


class BlogPostType(Enum):
    COMMON = 1
    CAROUSEL = 2
    IMPORTANT = 3
    PROJECTS = 4
    ABOUT = 5
    LESSON = 21


class TeamPostType(Enum):
    TEAM = 6
    CHIEF = 7
    STUDENT = 8


class EmailPostType(Enum):
    REGISTRATION = 9
    FORGOT = 10
    SPAM = 11
    MEETING_REGISTRATION = 12
    MEETING_THESIS = 13
    MEETING_SPAM = 14

    @property
    def is_meeting(self):
        return self.name in ('MEETING_REGISTRATION', 'MEETING_THESIS', 'MEETING_FORGOT', 'MEETING_SPAM')


class MeetingPostType(Enum):
    MEETING = 15
    REGISTRATION = 16
    COMMON = 17
    SUBMISSION = 22


class ThesisPostType(Enum):
    PLENARY = 20
    LECTURE = 25
    KEYNOTE = 24
    ORAL = 18
    SHORTCOMM = 26
    POSTER = 19
    EXTRAMURAL = 23

    @property
    def fancy(self):
        names = dict(PLENARY='Plenary', LECTURE='Lecture', KEYNOTE='Key-note', ORAL='Oral',
                     SHORTCOMM='Short Communication', POSTER='Poster', EXTRAMURAL='Extramural participation')
        return names[self.name]

    @staticmethod
    def thesis_types(part_type):
        if part_type == MeetingPartType.ORAL:
            return [ThesisPostType.PLENARY, ThesisPostType.LECTURE, ThesisPostType.KEYNOTE, ThesisPostType.ORAL,
                    ThesisPostType.SHORTCOMM]
        elif part_type == MeetingPartType.EXTRAMURAL:
            return [ThesisPostType.EXTRAMURAL]

        return [ThesisPostType.POSTER]

    @property
    def participation_type(self):
        if self == ThesisPostType.POSTER:
            return MeetingPartType.POSTER
        elif self == ThesisPostType.EXTRAMURAL:
            return MeetingPartType.EXTRAMURAL

        return MeetingPartType.ORAL


class MeetingPartType(Enum):
    ORAL = 1
    LISTENER = 2
    POSTER = 3
    EXTRAMURAL = 4

    @property
    def fancy(self):
        names = dict(ORAL='Oral/Plenary Presentation', POSTER='Poster Presentation', LISTENER='Without Presentation',
                     EXTRAMURAL='Extramural Participation')
        return names[self.name]


class Glyph(Enum):
    COMMON = 'file'
    CAROUSEL = 'camera'
    IMPORTANT = 'bullhorn'
    PROJECTS = 'hdd'
    ABOUT = 'eye-open'

    TEAM = 'knight'
    CHIEF = 'queen'
    STUDENT = 'pawn'

    MEETING = 'resize-small'

    REGISTRATION = 'send'
    FORGOT = 'send'
    SPAM = 'send'
    MEETING_REGISTRATION = 'send'
    MEETING_THESIS = 'send'
    MEETING_SPAM = 'send'

    ORAL = 'blackboard'
    POSTER = 'blackboard'
    PLENARY = 'blackboard'
    LECTURE = 'blackboard'
    KEYNOTE = 'blackboard'
    SHORTCOMM = 'blackboard'
    EXTRAMURAL = 'blackboard'

    LESSON = 'education'


class FormRoute(Enum):
    LOGIN = 1
    REGISTER = 2
    FORGOT = 3
    EDIT_PROFILE = 4
    LOGOUT_ALL = 5
    CHANGE_PASSWORD = 6
    NEW_BLOG_POST = 7
    NEW_EMAIL_TEMPLATE = 8
    NEW_MEETING_PAGE = 9
    NEW_MEMBER_PAGE = 10
    BAN_USER = 11
    CHANGE_USER_ROLE = 12

    @staticmethod
    def get(action):
        if 1 <= action <= 12:
            return FormRoute(action)
        return None

    def is_login(self):
        return 1 <= self.value <= 3

    def is_profile(self):
        return 4 <= self.value <= 12


class ProfileDegree(Enum):
    NO_DEGREE = 1
    PHD = 2
    SCID = 3

    @property
    def fancy(self):
        names = dict(NO_DEGREE='No Degree', PHD='Doctor of Philosophy', SCID='Doctor of Science')
        return names[self.name]


class ProfileStatus(Enum):
    COMMON = 1
    FOREIGN = 2
    RUS_SCIENTIST = 3
    RUS_YOUNG = 4
    PHD_STUDENT = 5
    STUDENT = 6
    INTERN = 7

    @property
    def fancy(self):
        names = dict(COMMON='Common', FOREIGN='Foreign participant', PHD_STUDENT='Ph.D. student',
                     RUS_SCIENTIST='Russian Scientist (from 40 year old)', STUDENT='Student', INTERN='Intern',
                     RUS_YOUNG='Russian young scientist (up to 39 year old)')
        return names[self.name]
