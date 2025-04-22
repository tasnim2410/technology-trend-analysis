from flask_sqlalchemy import SQLAlchemy

# Initialize SQLAlchemy
# In your app, import `db` and call db.init_app(app)
db = SQLAlchemy()

class RawPatent(db.Model):
    __tablename__ = 'raw_patents'
    id = db.Column('No', db.Integer, primary_key=True)
    title = db.Column('Title', db.String(255), nullable=False)
    inventors = db.Column('Inventors', db.Text, nullable=True)
    applicants = db.Column('Applicants', db.Text, nullable=True)
    publication_number = db.Column('Publication number', db.String(100), nullable=False)
    earliest_priority = db.Column('Earliest priority', db.Date, nullable=True)
    ipc = db.Column('IPC', db.Text, nullable=True)
    cpc = db.Column('CPC', db.Text, nullable=True)
    publication_date = db.Column('Publication date', db.Date, nullable=True)
    first_publication_date = db.Column('first publication date', db.Date, nullable=True)  # Added
    second_publication_date = db.Column('second publication date', db.String(50), nullable=True)  # Added
    first_filing_year = db.Column('first filing year', db.Integer, nullable=True)  # Added
    earliest_priority_year = db.Column('earliest priority year', db.Integer, nullable=True)  # Added
    applicant_country = db.Column('applicant country', db.String(2), nullable=True)  # Added
    family_number = db.Column('Family number', db.BigInteger, nullable=True)
    family_jurisdictions = db.Column(db.ARRAY(db.String(2)), nullable=True)
    family_members = db.Column(db.ARRAY(db.String(50)), nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'inventors': self.inventors,
            'applicants': self.applicants,
            'publication_number': self.publication_number,
            'earliest_priority': self.earliest_priority.isoformat() if self.earliest_priority else None,
            'ipc': self.ipc,
            'cpc': self.cpc,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'first_publication_date': self.first_publication_date.isoformat() if self.first_publication_date else None,
            'second_publication_date': self.second_publication_date,
            'first_filing_year': self.first_filing_year,
            'earliest_priority_year': self.earliest_priority_year,
            'applicant_country': self.applicant_country,
            'family_number': self.family_number,
            'family_jurisdictions': self.family_jurisdictions,
            'family_members' : self.family_members
        }
