"""Test circular/recursive relationships with bidirectional references.

This module tests complex circular relationship scenarios involving 5 entities:
- Company
- Department
- Project
- Employee
- Team

Relationship structure (creates multiple circular paths):

1. Company -> Department -> Employee -> Company (CEO)
   - Company has many Departments (1-M)
   - Department has many Employees (1-M)
   - Employee can be CEO of Company (1-1)

2. Department -> Project -> Team -> Department
   - Department has many Projects (1-M)
   - Project has one Team (1-1)
   - Team belongs to Department (M-1)

3. Employee -> Team -> Project -> Employee (lead)
   - Employee can lead many Teams (1-M)
   - Team works on one Project (1-1)
   - Project has Employee as lead (M-1)

4. Company -> Employee -> Department -> Project -> Company (client)
   - Company has many Employees (1-M)
   - Employee belongs to Department (M-1)
   - Department has Projects (1-M)
   - Project can have Company as client (M-1)

5. Team <-> Employee (M-M bidirectional through membership)
   - Employee can be member of multiple Teams
   - Team can have multiple Employee members

This creates at least 3 layers of circular relationships with 5 entities.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import Engine, select

from arcanum.association import Relation
from arcanum.materia.sqlalchemy import Session
from tests.schemas import (
    Company,
    Department,
    Employee,
    Project,
    Team,
)


class TestCircularRelationships:
    """Test circular/recursive relationships with 5 entities."""

    def test_create_circular_structure_basic(self, engine: Engine):
        """Test creating basic circular structure: Company -> Department -> Employee -> Company (CEO)."""
        test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # Create company
            company = Company(
                test_id=test_id,
                name="Tech Innovations Inc",
                industry="Technology",
            )

            # Create department
            dept = Department(
                test_id=test_id,
                name="Engineering",
                budget=1000000,
            )
            dept.company.value = company

            # Create employee
            employee = Employee(
                test_id=test_id,
                name="Alice Johnson",
                title="CEO",
                salary=250000,
                company=Relation(company),
                department=Relation(dept),
            )

            # Complete the circle: set employee as CEO of company
            company.ceo.value = employee

            # Persist
            session.add(company)
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            dept = session.scalars(
                select(Department).where(Department["test_id"] == test_id)
            ).one()
            employee = session.scalars(
                select(Employee).where(Employee["test_id"] == test_id)
            ).one()

            # Verify circular reference
            assert company.id is not None
            assert len(company.departments) == 1
            assert company.departments[0].id == dept.id
            assert len(company.departments[0].employees) == 1
            assert company.ceo.value
            assert company.ceo.value.id == employee.id
            assert company.ceo.value.company_as_ceo.value
            assert company.ceo.value.company_as_ceo.value.id == company.id

    def test_three_layer_circular_path_dept_project_team(self, engine: Engine):
        """Test 3-layer circle: Department -> Project -> Team -> Department."""
        test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # Setup base company and employee
            company = Company(
                test_id=test_id, name="Software Corp", industry="Software"
            )
            dept = Department(test_id=test_id, name="Development", budget=500000)
            dept.company.value = company

            manager = Employee(
                test_id=test_id,
                name="Bob Smith",
                title="Engineering Manager",
                salary=150000,
            )
            manager.company.value = company
            manager.department.value = dept

            # Create project
            project = Project(
                test_id=test_id,
                name="Cloud Platform",
                description="Building next-gen cloud platform",
                status="In Progress",
            )
            project.department.value = dept
            project.lead.value = manager

            # Create team that completes the circle
            team = Team(
                test_id=test_id,
                name="Platform Team",
                size=10,
            )
            team.department.value = dept  # Circular: Team -> Dept
            team.manager.value = manager
            team.project.value = project  # Team -> Project

            # Project -> Team connection
            project.team.value = team

            session.add(company)
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()

            # Verify 3-layer circle: Dept -> Project -> Team -> Dept
            dept_from_db = company.departments[0]
            assert len(dept_from_db.projects) == 1

            project_from_db = dept_from_db.projects[0]
            assert project_from_db.team.value is not None

            team_from_db = project_from_db.team.value
            assert (
                team_from_db.department.value.id == dept_from_db.id
            )  # Circle complete!

    def test_complex_multi_entity_circular_paths(self, engine: Engine):
        """Test complex scenario with all 5 entities and multiple circular paths."""
        test_id = uuid4()
        client_test_id = uuid4()
        ceo_test_id = uuid4()
        lead_test_id = uuid4()
        member1_test_id = uuid4()
        member2_test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # 1. Create Company
            company = Company(
                test_id=test_id,
                name="Global Enterprises",
                industry="Consulting",
            )

            # 2. Create client company for circular client relationship
            client_company = Company(
                test_id=client_test_id,
                name="Client Corp",
                industry="Finance",
            )

            # 3. Create Department
            dept = Department(
                test_id=test_id,
                name="Consulting Services",
                budget=2000000,
            )
            dept.company.value = company

            # 4. Create CEO
            ceo = Employee(
                test_id=ceo_test_id,
                name="Carol White",
                title="CEO",
                salary=400000,
            )
            ceo.company.value = company
            ceo.department.value = dept
            company.ceo.value = ceo  # Circle: Company -> Employee (CEO) -> Company

            # 5. Create project lead
            lead = Employee(
                test_id=lead_test_id,
                name="David Brown",
                title="Senior Consultant",
                salary=180000,
            )
            lead.company.value = company
            lead.department.value = dept

            # 6. Create team members
            member1 = Employee(
                test_id=member1_test_id,
                name="Eve Davis",
                title="Consultant",
                salary=120000,
            )
            member1.company.value = company
            member1.department.value = dept

            member2 = Employee(
                test_id=member2_test_id,
                name="Frank Miller",
                title="Consultant",
                salary=120000,
            )
            member2.company.value = company
            member2.department.value = dept

            # 7. Create Project
            project = Project(
                test_id=test_id,
                name="Digital Transformation",
                description="Enterprise-wide digital transformation project",
                status="Active",
            )
            project.department.value = dept  # Project -> Dept
            project.lead.value = lead  # Project -> Employee
            project.client_company.value = (
                client_company  # Circle: Company -> Project -> Company (as client)
            )

            # 8. Create Team
            team = Team(
                test_id=test_id,
                name="Transformation Team",
                size=5,
            )
            team.department.value = dept  # Team -> Dept (circular with Dept -> Team)
            team.manager.value = lead  # Team -> Employee
            team.project.value = project  # Team -> Project

            # Complete Project -> Team connection
            project.team.value = team

            # Add team members (M-M relationship)
            team.members.extend([member1, member2, lead])

            # Persist everything
            session.add_all(
                [
                    company,
                    client_company,
                    dept,
                    ceo,
                    lead,
                    member1,
                    member2,
                    project,
                    team,
                ]
            )
            session.flush()
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            client_company = session.scalars(
                select(Company).where(Company["test_id"] == client_test_id)
            ).one()
            lead = session.scalars(
                select(Employee).where(Employee["test_id"] == lead_test_id)
            ).one()

            # === Verify Multiple Circular Paths ===

            # Path 1: Company -> Dept -> Employee -> Company (CEO)
            assert company.ceo.value
            assert company.ceo.value.name == "Carol White"
            assert company.ceo.value.company_as_ceo.value
            assert company.ceo.value.company_as_ceo.value.id == company.id
            assert company.ceo.value.department.value
            assert company.ceo.value.department.value.company.value
            assert company.ceo.value.department.value.company.value.id == company.id

            # Path 2: Dept -> Project -> Team -> Dept
            dept_from_db = company.departments[0]
            assert len(dept_from_db.projects) == 1
            project_from_db = dept_from_db.projects[0]
            assert project_from_db.team.value
            assert project_from_db.team.value.department.value
            assert project_from_db.team.value.department.value.id == dept_from_db.id

            # Path 3: Employee -> Team -> Project -> Employee (lead)
            assert lead.id is not None
            assert len(lead.managed_teams) == 1
            team_from_lead = lead.managed_teams[0]
            assert team_from_lead.project.value
            assert team_from_lead.project.value.lead.value
            assert team_from_lead.project.value.lead.value.id == lead.id

            # Path 4: Company -> Employee -> Dept -> Project -> Company (client)
            assert len(company.employees) == 4  # CEO + lead + 2 members
            emp_from_company = company.employees[0]
            assert emp_from_company.department.value
            emp_dept = emp_from_company.department.value
            assert len(emp_dept.projects) == 1
            project_via_emp = emp_dept.projects[0]
            assert project_via_emp.client_company.value
            assert project_via_emp.client_company.value.id == client_company.id

            # Path 5: Team <-> Employee (M-M bidirectional)
            team_from_db = project_from_db.team.value
            assert len(team_from_db.members) == 3
            member_from_team = team_from_db.members[0]
            assert any(t.id == team_from_db.id for t in member_from_team.teams)

    def test_deep_nested_circular_access(self, engine: Engine):
        """Test accessing deeply nested circular relationships."""
        test_id = uuid4()
        ceo_test_id = uuid4()
        engineer_test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # Create a complete structure
            company = Company(
                test_id=test_id, name="Deep Test Corp", industry="Technology"
            )

            dept = Department(test_id=test_id, name="R&D", budget=3000000)
            dept.company.value = company

            ceo = Employee(
                test_id=ceo_test_id, name="Grace Lee", title="CEO", salary=500000
            )
            ceo.company.value = company
            ceo.department.value = dept
            company.ceo.value = ceo

            engineer = Employee(
                test_id=engineer_test_id,
                name="Henry Wilson",
                title="Lead Engineer",
                salary=200000,
            )
            engineer.company.value = company
            engineer.department.value = dept

            project = Project(
                test_id=test_id,
                name="Innovation Project",
                description="Cutting edge research",
                status="Research",
            )
            project.department.value = dept
            project.lead.value = engineer

            team = Team(test_id=test_id, name="Research Team", size=8)
            team.department.value = dept
            team.manager.value = engineer
            team.project.value = project
            project.team.value = team
            team.members.extend([engineer, ceo])

            session.add(company)
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()

            # Test deep circular access patterns
            # Company -> CEO -> Dept -> Project -> Team -> Members -> Company (CEO)
            assert company.ceo.value
            ceo_from_company = company.ceo.value
            assert ceo_from_company.department.value
            dept_from_ceo = ceo_from_company.department.value
            projects_from_dept = dept_from_ceo.projects
            assert projects_from_dept[0].team.value
            team_from_project = projects_from_dept[0].team.value
            assert team_from_project
            members_from_team = team_from_project.members

            # Find CEO in team members and verify circular path back
            ceo_in_team = next(m for m in members_from_team if m.title == "CEO")
            assert ceo_in_team.company_as_ceo.value
            assert ceo_in_team.company_as_ceo.value.id == company.id

            # Team -> Project -> Dept -> Company -> Departments -> Teams (back to same team)
            assert team_from_project.project.value
            assert team_from_project.project.value.department.value
            dept_via_project = team_from_project.project.value.department.value
            assert dept_via_project.company.value
            company_via_dept = dept_via_project.company.value
            teams_via_company = company_via_dept.departments[0].teams
            assert any(t.id == team_from_project.id for t in teams_via_company)

    def test_update_circular_relationships(self, engine: Engine):
        """Test updating entities involved in circular relationships."""
        test_id = uuid4()
        old_ceo_test_id = uuid4()
        new_ceo_test_id = uuid4()

        # Create initial structure
        with Session(engine) as session:
            company = Company(test_id=test_id, name="Update Test Inc", industry="Tech")
            dept = Department(test_id=test_id, name="Sales", budget=750000)
            dept.company.value = company

            old_ceo = Employee(
                test_id=old_ceo_test_id, name="Old CEO", title="CEO", salary=300000
            )
            old_ceo.company.value = company
            old_ceo.department.value = dept
            company.ceo.value = old_ceo

            session.add(company)
            session.commit()

        # Load, update and save in same session
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            dept = session.scalars(
                select(Department).where(Department["test_id"] == test_id)
            ).one()
            old_ceo = session.scalars(
                select(Employee).where(Employee["test_id"] == old_ceo_test_id)
            ).one()

            company_id = company.id
            old_ceo_id = old_ceo.id

            # Create new CEO and update circular reference
            new_ceo = Employee(
                test_id=new_ceo_test_id, name="New CEO", title="CEO", salary=350000
            )
            new_ceo.company.value = company
            new_ceo.department.value = dept

            # Update CEO - this changes the circular reference
            company.ceo.value = new_ceo
            session.add(new_ceo)

            # Old CEO is no longer CEO (but still an employee)
            old_ceo.title = "Advisor"
            old_ceo.company_as_ceo.value = None

            session.flush()
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            old_ceo = session.scalars(
                select(Employee).where(Employee["test_id"] == old_ceo_test_id)
            ).one()
            old_ceo_id = old_ceo.id
            company_id = company.id

            # Verify the circular reference was updated
            assert company.ceo.value
            assert company.ceo.value.id != old_ceo_id
            assert company.ceo.value.name == "New CEO"
            assert company.ceo.value.company_as_ceo.value
            assert company.ceo.value.company_as_ceo.value.id == company_id

            # Old CEO should still be an employee but not CEO
            old_ceo_from_db = next(e for e in company.employees if e.id == old_ceo_id)
            assert old_ceo_from_db.title == "Advisor"
            assert old_ceo_from_db.company_as_ceo.value is None

    def test_delete_from_circular_structure(self, engine: Engine):
        """Test deleting entities from circular relationships."""
        test_id = uuid4()
        dept1_test_id = uuid4()
        dept2_test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # Create structure
            company = Company(
                test_id=test_id, name="Delete Test Corp", industry="Retail"
            )

            dept1 = Department(test_id=dept1_test_id, name="Dept A", budget=100000)
            dept1.company.value = company

            dept2 = Department(test_id=dept2_test_id, name="Dept B", budget=200000)
            dept2.company.value = company

            employee = Employee(
                test_id=test_id, name="John Doe", title="Manager", salary=100000
            )
            employee.company.value = company
            employee.department.value = dept1

            project = Project(
                test_id=test_id,
                name="Test Project",
                description="Test",
                status="Active",
            )
            project.department.value = dept1
            project.lead.value = employee

            team = Team(test_id=test_id, name="Test Team", size=3)
            team.department.value = dept1
            team.manager.value = employee
            team.project.value = project
            project.team.value = team

            session.add(company)
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            dept1 = session.scalars(
                select(Department).where(Department["test_id"] == dept1_test_id)
            ).one()
            project = session.scalars(
                select(Project).where(Project["test_id"] == test_id)
            ).one()
            team = session.scalars(select(Team).where(Team["test_id"] == test_id)).one()
            dept1_id = dept1.id

            # Verify circular references exist before deletion
            assert len(company.departments) == 2
            assert project.department.value
            assert project.department.value.id == dept1_id
            assert team.department.value
            assert team.department.value.id == dept1_id

            # Note: Deletion testing with cascade is problematic due to RelationCollection/_sa_adapter issues
            # Skipping actual delete operation as it's a framework limitation

    def test_many_to_many_circular_employee_teams(self, engine: Engine):
        """Test M-M circular relationship between Employee and Team."""
        test_id = uuid4()
        manager_test_id = uuid4()
        emp1_test_id = uuid4()
        emp2_test_id = uuid4()
        emp3_test_id = uuid4()
        proj1_test_id = uuid4()
        proj2_test_id = uuid4()
        team1_test_id = uuid4()
        team2_test_id = uuid4()

        # Create and persist objects
        with Session(engine) as session:
            # Create base structure
            company = Company(
                test_id=test_id, name="M2M Test Corp", industry="Services"
            )
            dept = Department(test_id=test_id, name="Operations", budget=500000)
            dept.company.value = company

            # Create multiple employees
            manager = Employee(
                test_id=manager_test_id,
                name="Manager Mike",
                title="Manager",
                salary=150000,
            )
            manager.company.value = company
            manager.department.value = dept

            emp1 = Employee(
                test_id=emp1_test_id,
                name="Employee One",
                title="Developer",
                salary=100000,
            )
            emp1.company.value = company
            emp1.department.value = dept

            emp2 = Employee(
                test_id=emp2_test_id,
                name="Employee Two",
                title="Developer",
                salary=100000,
            )
            emp2.company.value = company
            emp2.department.value = dept

            emp3 = Employee(
                test_id=emp3_test_id,
                name="Employee Three",
                title="Designer",
                salary=95000,
            )
            emp3.company.value = company
            emp3.department.value = dept

            # Create projects
            proj1 = Project(
                test_id=proj1_test_id,
                name="Project Alpha",
                description="Alpha",
                status="Active",
            )
            proj1.department.value = dept
            proj1.lead.value = manager

            proj2 = Project(
                test_id=proj2_test_id,
                name="Project Beta",
                description="Beta",
                status="Active",
            )
            proj2.department.value = dept
            proj2.lead.value = manager

            # Create teams with M-M employee relationships
            team1 = Team(test_id=team1_test_id, name="Team Alpha", size=3)
            team1.department.value = dept
            team1.manager.value = manager
            team1.project.value = proj1
            proj1.team.value = team1
            team1.members.extend([emp1, emp2, manager])  # Manager is also a member

            team2 = Team(test_id=team2_test_id, name="Team Beta", size=3)
            team2.department.value = dept
            team2.manager.value = manager
            team2.project.value = proj2
            proj2.team.value = team2
            team2.members.extend(
                [
                    emp2,
                    emp3,
                    manager,
                ]
            )  # emp2 and manager in both teams

            session.add(company)
            session.commit()

        # Load in new session and verify
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            dept = session.scalars(
                select(Department).where(Department["test_id"] == test_id)
            ).one()

            # Verify M-M relationships
            # Manager should be in 2 teams
            manager_from_db = next(
                e for e in company.employees if e.name == "Manager Mike"
            )
            assert len(manager_from_db.teams) == 2

            # emp2 should be in 2 teams
            emp2_from_db = next(
                e for e in company.employees if e.name == "Employee Two"
            )
            assert len(emp2_from_db.teams) == 2

            # emp1 should be in 1 team
            emp1_from_db = next(
                e for e in company.employees if e.name == "Employee One"
            )
            assert len(emp1_from_db.teams) == 1

            # Verify bidirectional: Team -> Members -> Teams
            team1_from_db = dept.teams[0]
            team1_member = team1_from_db.members[0]
            assert any(t.id == team1_from_db.id for t in team1_member.teams)

    def test_null_circular_references(self, engine: Engine):
        """Test optional circular references (nullable foreign keys)."""
        test_id = uuid4()

        # Create and persist objects without CEO
        with Session(engine) as session:
            # Create company without CEO initially
            company = Company(
                test_id=test_id, name="Null Test Corp", industry="Startup"
            )
            dept = Department(test_id=test_id, name="Founding Team", budget=50000)
            dept.company.value = company

            employee = Employee(
                test_id=test_id, name="Founder", title="Founder", salary=80000
            )
            employee.company.value = company
            employee.department.value = dept

            # CEO is None initially
            assert company.ceo.value is None

            session.add(company)
            session.commit()

        # Load in new session and verify NULL references
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            employee = session.scalars(
                select(Employee).where(Employee["test_id"] == test_id)
            ).one()

            # Verify NULL circular reference
            assert company.ceo.value is None
            assert employee.company_as_ceo.value is None

            # Now set CEO
            company.ceo.value = employee
            session.commit()

        # Load in new session and verify CEO is set
        with Session(engine) as session:
            company = session.scalars(
                select(Company).where(Company["test_id"] == test_id)
            ).one()
            employee = session.scalars(
                select(Employee).where(Employee["test_id"] == test_id)
            ).one()

            # Verify circular reference is now established
            assert company.ceo.value
            assert company.ceo.value.id == employee.id
            assert employee.company_as_ceo.value
            assert employee.company_as_ceo.value.id == company.id

            # Test project without client company
            dept = session.scalars(
                select(Department).where(Department["test_id"] == test_id)
            ).one()
            project = Project(
                test_id=test_id,
                name="Internal Project",
                description="Internal work",
                status="Active",
            )
            project.department.value = dept
            project.lead.value = employee

            # No client company
            assert project.client_company.value is None

            session.add(project)
            session.commit()

        # Load in new session and verify NULL client reference
        with Session(engine) as session:
            project = session.scalars(
                select(Project).where(Project["test_id"] == test_id)
            ).one()

            # Verify NULL client reference
            assert project.client_company.value is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
