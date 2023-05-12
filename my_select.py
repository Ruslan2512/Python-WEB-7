from sqlalchemy import func, desc, select
from sqlalchemy import and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()

    return result


def select_2(discipline_id: int):
    result = session.query(Discipline.name,
                            Student.fullname,
                            func.round(func.avg(Grade.grade), 2).label('avg_grade')
                            ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()

    return result


def select_3(discipline_id: int):
    result = session.query(Discipline.name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()

    return result


def select_4(group_id: int):
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .filter(Student.group_id == group_id) \
        .order_by(desc('avg_grade')) \
        .all()

    return result


def select_5(teacher_id: int):
    result = session.query(Discipline.name,
                           Discipline.id
                           ) \
        .select_from(Discipline) \
        .join(Teacher) \
        .filter(Discipline.teacher_id == teacher_id) \
        .all()

    return result


def select_6(group_id: int):
    result = session.query(Student.fullname
                           ) \
        .select_from(Student) \
        .filter(Student.group_id == group_id) \
        .all()

    return result


def select_7(group_id: int, discipline_id: int):
    result = session.query(Group.name,
                           Grade.grade
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .filter(Group.id == group_id, Grade.discipline_id == discipline_id) \
        .all()

    return result


def select_8(discipline_id: int, teacher_id: int):
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Discipline) \
        .filter(Grade.discipline_id == discipline_id, Discipline.teacher_id == teacher_id) \
        .order_by(desc('avg_grade')) \
        .all()

    return result


def select_9(student_id: int):
    result = session.query(Discipline.name) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Student) \
        .filter(Grade.student_id == student_id) \
        .group_by(Discipline.name) \
        .all()

    return result


def select_10(discipline_id: int, teacher_id: int):
    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Grade) \
        .filter(Grade.discipline_id == discipline_id, Discipline.teacher_id == teacher_id) \
        .group_by(Discipline.name) \
        .all()

    return result


def select_11(student_id: int, student_name_id: int):
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Discipline) \
        .filter(Grade.student_id == student_id, Discipline.teacher_id == student_name_id) \
        .order_by(desc('avg_grade')) \
        .all()

    return result


def select_12(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())
    result = session.query(Discipline.name,
                           Student.fullname,
                           Group.name,
                           Grade.date_of,
                           Grade.grade
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()

    return result


if __name__ == '__main__':
    print(select_1())
    print(select_2(2))
    print(select_3(2))
    print(select_4(2))
    print(select_5(2))
    print(select_6(2))
    print(select_7(2, 5))
    print(select_8(5, 2))
    print(select_9(49))
    print(select_10(5, 2))
    print(select_11(37, 2))
    print(select_12(5, 1))


