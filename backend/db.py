from flask_sqlalchemy import SQLAlchemy

# Initialize SQLAlchemy
# In your app, import `db` and call db.init_app(app)
db = SQLAlchemy()

class RawPatent(db.Model):
    __tablename__ = 'raw_patents'
    id = db.Column('No', db.Integer, primary_key=True)
    title = db.Column('Title', db.Text, nullable=False)
    inventors = db.Column('Inventors', db.Text, nullable=True)
    applicants = db.Column('Applicants', db.Text, nullable=True)
    publication_number = db.Column('Publication number', db.String(100), nullable=False)
    earliest_priority = db.Column('Earliest priority', db.Date, nullable=True)
    earliest_publication = db.Column('earliest_publication', db.Date, nullable=True)
    ipc = db.Column('IPC', db.Text, nullable=True)
    cpc = db.Column('CPC', db.Text, nullable=True)
    publication_date = db.Column('Publication date', db.Date, nullable=True)
    first_publication_date = db.Column('first_publication_date', db.Date, nullable=True)
    second_publication_date = db.Column('second_publication_date', db.String(50), nullable=True)
    first_filing_year = db.Column('first_filing_year', db.Integer, nullable=True)
    earliest_priority_year = db.Column('earliest_priority_year', db.Integer, nullable=True)
    applicant_country = db.Column('applicant_country', db.String(2), nullable=True)
    family_number = db.Column('Family number', db.BigInteger, nullable=True)
    family_jurisdictions = db.Column(db.ARRAY(db.String(2)), nullable=True)
    family_members = db.Column(db.ARRAY(db.String(50)), nullable=True)
    first_publication_number = db.Column('first_publication_number', db.String(50), nullable=True)
    second_publication_number = db.Column('second_publication_number', db.String(50), nullable=True)
    first_publication_country = db.Column('first_publication_country', db.String(2), nullable=True)
    second_publication_country = db.Column('second_publication_country', db.String(2), nullable=True)

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
            'family_members' : self.family_members,
            'first_publication_number': self.first_publication_number,
            'second_publication_number': self.second_publication_number,
            'first_publication_country': self.first_publication_country,
            'second_publication_country': self.second_publication_country,
            'earliest_publication': self.earliest_publication.isoformat() if self.earliest_publication else None
        }
class SearchKeyword(db.Model):
    __tablename__ = 'search_keywords'
    id = db.Column(db.Integer, primary_key=True)
    search_id = db.Column(db.String(36), nullable=False)
    field = db.Column(db.String(50), nullable=False)
    keyword = db.Column(db.String(255), nullable=False)
    


class ResearchData(db.Model):
    __tablename__ = 'research_data'
    id = db.Column(db.BigInteger, primary_key=True)
    paper_id = db.Column(db.String(255), unique=True, nullable=False)
    title = db.Column(db.Text, nullable=False)  # Changed to Text
    abstract = db.Column(db.Text, nullable=True)
    publication_venue_name = db.Column(db.Text, nullable=True)  # Changed to Text
    publication_venue_type = db.Column(db.String(100), nullable=True)  # Increased to 100
    year = db.Column(db.Integer, nullable=True)
    reference_count = db.Column(db.BigInteger, nullable=True)
    citation_count = db.Column(db.Integer, nullable=True)
    influential_citation_count = db.Column(db.Integer, nullable=True)
    fields_of_study = db.Column(db.ARRAY(db.String), nullable=True)
    publication_types = db.Column(db.ARRAY(db.String), nullable=True)
    publication_date = db.Column(db.Date, nullable=True)
    authors = db.Column(db.Text, nullable=True)  # Changed to Text

    def to_dict(self):
        return {
            'id': self.id,
            'paper_id': self.paper_id,
            'title': self.title,
            'abstract': self.abstract,
            'publication_venue_name': self.publication_venue_name,
            'publication_venue_type': self.publication_venue_type,
            'year': self.year,
            'reference_count': self.reference_count,
            'citation_count': self.citation_count,
            'influential_citation_count': self.influential_citation_count,
            'fields_of_study': self.fields_of_study,
            'publication_types': self.publication_types,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'authors': self.authors
        }



class ImpactFactor(db.Model):
    __tablename__ = 'impact_factors'
    # Add the primary key column
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(500), nullable=False)
    abbrev_name = db.Column(db.String(255))
    issn = db.Column(db.String(9))
    jif_5yr = db.Column(db.Numeric(6,3))
    subcategory = db.Column(db.String(150))
    field = db.Column(db.String(50))
    
    # Existing table arguments (if any)
    __table_args__ = (
        db.Index('idx_field_jif', 'field', 'jif_5yr'),
        db.UniqueConstraint('issn', name='uq_issn_pair')
    )

    def to_dict(self):
        return {
            'abbrev_name': self.abbrev_name,
            'issn': self.issn,
            'jif_5yr': float(self.jif_5yr) if self.jif_5yr else None,
            'subcategory': self.subcategory,
            'field': self.field
        }
        


class ResearchData2(db.Model):
    __tablename__ = 'research_data2'

    id = db.Column(db.BigInteger, primary_key=True)
    paper_id = db.Column(db.String(255), unique=True, nullable=False)
    title = db.Column(db.Text, nullable=False)
    abstract = db.Column(db.Text, nullable=True)
    publication_venue_name = db.Column(db.Text, nullable=True)
    publication_venue_type = db.Column(db.String(100), nullable=True)
    year = db.Column(db.Integer, nullable=True)
    reference_count = db.Column(db.BigInteger, nullable=True)
    citation_count = db.Column(db.Integer, nullable=True)
    influential_citation_count = db.Column(db.Integer, nullable=True)
    fields_of_study = db.Column(db.ARRAY(db.String), nullable=True)
    publication_types = db.Column(db.ARRAY(db.String), nullable=True)
    publication_date = db.Column(db.Date, nullable=True)
    authors = db.Column(db.Text, nullable=True)

    
    subcategory = db.Column(db.String(255), nullable=True)
    field = db.Column(db.String(255), nullable=True)
    jif_5years = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'paper_id': self.paper_id,
            'title': self.title,
            'abstract': self.abstract,
            'publication_venue_name': self.publication_venue_name,
            'publication_venue_type': self.publication_venue_type,
            'year': self.year,
            'reference_count': self.reference_count,
            'citation_count': self.citation_count,
            'influential_citation_count': self.influential_citation_count,
            'fields_of_study': self.fields_of_study,
            'publication_types': self.publication_types,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'authors': self.authors,
            'subcategory': self.subcategory,
            'field': self.field,
            'jif_5years': self.jif_5years
        }



     
        
class ResearchData3(db.Model):
    __tablename__ = 'research_data3'

    id = db.Column(db.BigInteger, primary_key=True)
    paper_id = db.Column(db.String(255), nullable=False)
    doi = db.Column(db.String(255), nullable=True)
    title = db.Column(db.Text, nullable=False)
    official_title = db.Column(db.Text, nullable=True)
    abstract = db.Column(db.Text, nullable=True)
    relevance = db.Column(db.Float, nullable=True)
    year = db.Column(db.Integer, nullable=True)
    publication_date = db.Column(db.Date, nullable=True)
    publication_types = db.Column(db.ARRAY(db.String), nullable=True)
    source_type = db.Column(db.String(50), nullable=True)
    publication_venue_name = db.Column(db.Text, nullable=True)
    publication_venue_type = db.Column(db.String(100), nullable=True)
    journal_name = db.Column(db.String(255), nullable=True)
    journal_issn_l = db.Column(db.String(50), nullable=True)
    journal_issn_l_clean = db.Column(db.String(50), nullable=True)
    issn = db.Column(db.String(50), nullable=True)
    publisher = db.Column(db.String(255), nullable=True)
    publisher_hierarchy = db.Column(db.ARRAY(db.String), nullable=True)
    main_topic = db.Column(db.String(100), nullable=True)
    fields_of_study = db.Column(db.ARRAY(db.String), nullable=True)
    academic_domain = db.Column(db.String(100), nullable=True)
    subcategory = db.Column(db.String(100), nullable=True)
    keyword_relevance_score = db.Column(db.Float, nullable=True)
    keywords = db.Column(db.ARRAY(db.String), nullable=True)
    all_journal_sources = db.Column(db.ARRAY(db.String), nullable=True)
    authorships_countries = db.Column(db.ARRAY(db.String), nullable=True)
    reference_count = db.Column(db.BigInteger, nullable=True)
    citation_count = db.Column(db.Integer, nullable=True)
    influential_citation_count = db.Column(db.Integer, nullable=True)
    field_weighted_citation_impact = db.Column(db.Float, nullable=True)
    jif_5years = db.Column(db.Float, nullable=True)
    field = db.Column(db.String(255), nullable=True) 

    def to_dict(self):
        return {
            'id': self.id,
            'paper_id': self.paper_id,
            'doi': self.doi,
            'title': self.title,
            'official_title': self.official_title,
            'abstract': self.abstract,
            'relevance': self.relevance,
            'year': self.year,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'publication_types': self.publication_types,
            'source_type': self.source_type,
            'publication_venue_name': self.publication_venue_name,
            'publication_venue_type': self.publication_venue_type,
            'journal_name': self.journal_name,
            'journal_issn_l': self.journal_issn_l,
            'journal_issn_l_clean': self.journal_issn_l_clean,
            'issn': self.issn,
            'publisher': self.publisher,
            'publisher_hierarchy': self.publisher_hierarchy,
            'main_topic': self.main_topic,
            'fields_of_study': self.fields_of_study,
            'academic_domain': self.academic_domain,
            'subcategory': self.subcategory,
            'keyword_relevance_score': self.keyword_relevance_score,
            'keywords': self.keywords,
            'all_journal_sources': self.all_journal_sources,
            'authorships_countries': self.authorships_countries,
            'reference_count': self.reference_count,
            'citation_count': self.citation_count,
            'influential_citation_count': self.influential_citation_count,
            'field_weighted_citation_impact': self.field_weighted_citation_impact,
            'jif_5years': self.jif_5years,
            'field': self.field
        }
        
        
class PatentKeyword(db.Model):
    __tablename__ = 'patent_keywords'
    id = db.Column(db.Integer, primary_key=True)
    first_publication_number = db.Column(db.String(50), nullable=False, index=True)
    title = db.Column(db.Text, nullable=False)
    first_filing_year = db.Column(db.Integer, nullable=True)
    keywords = db.Column(db.ARRAY(db.String), nullable=True)  # Stores extracted keywords

    def to_dict(self):
        return {
            'id': self.id,
            'first_publication_number': self.first_publication_number,
            'title': self.title,
            'first_filing_year': self.first_filing_year,
            'keywords': self.keywords
        }   
    
    
class Window(db.Model):
    __tablename__ = 'windows'
    id = db.Column(db.Integer, primary_key=True)
    start_year = db.Column(db.Integer, nullable=False)
    end_year = db.Column(db.Integer, nullable=False)
    topics = db.relationship('Topic', backref='window', lazy=True)

class Topic(db.Model):
    __tablename__ = 'topics'
    id = db.Column(db.Integer, primary_key=True)
    window_id = db.Column(db.Integer, db.ForeignKey('windows.id'), nullable=False)
    topic_number = db.Column(db.Integer, nullable=False)
    words = db.Column(db.ARRAY(db.String), nullable=False)
    weights = db.Column(db.ARRAY(db.Float), nullable=False)
    
    
class Divergence(db.Model):
    __tablename__ = 'divergences'
    id = db.Column(db.Integer, primary_key=True)
    from_year = db.Column(db.Integer, nullable=False)
    to_year = db.Column(db.Integer, nullable=False)
    divergence = db.Column(db.Float, nullable=False)
    
    
    
class IPCClassification(db.Model):
    __tablename__ = 'ipc_classifications'
    cpc_symbol = db.Column(db.String(20), primary_key=True)
    classification_title = db.Column(db.Text, nullable=False)

    def to_dict(self):
        return {
            'cpc_symbol': self.cpc_symbol,
            'classification_title': self.classification_title
        }
        
        
        
class ClassifiedPatent(db.Model):
    __tablename__ = 'classified_patents'
    id = db.Column('No', db.Integer, primary_key=True)
    title = db.Column('Title', db.Text, nullable=False)
    inventors = db.Column('Inventors', db.Text, nullable=True)
    applicants = db.Column('Applicants', db.Text, nullable=True)
    publication_number = db.Column('Publication number', db.String(100), nullable=False)
    earliest_priority = db.Column('Earliest priority', db.Date, nullable=True)
    earliest_publication = db.Column('earliest_publication', db.Date, nullable=True)
    ipc = db.Column('IPC', db.Text, nullable=True)
    cpc = db.Column('CPC', db.Text, nullable=True)
    publication_date = db.Column('Publication date', db.Date, nullable=True)
    first_publication_date = db.Column('first_publication_date', db.Date, nullable=True)
    second_publication_date = db.Column('second_publication_date', db.String(50), nullable=True)
    first_filing_year = db.Column('first_filing_year', db.Integer, nullable=True)
    earliest_priority_year = db.Column('earliest_priority_year', db.Integer, nullable=True)
    applicant_country = db.Column('applicant_country', db.String(2), nullable=True)
    family_number = db.Column('Family number', db.BigInteger, nullable=True)
    family_jurisdictions = db.Column(db.ARRAY(db.String(2)), nullable=True)
    family_members = db.Column(db.ARRAY(db.String(50)), nullable=True)
    first_publication_number = db.Column('first_publication_number', db.String(50), nullable=True)
    second_publication_number = db.Column('second_publication_number', db.String(50), nullable=True)
    first_publication_country = db.Column('first_publication_country', db.String(2), nullable=True)
    second_publication_country = db.Column('second_publication_country', db.String(2), nullable=True)
    ipc_meaning = db.Column('ipc_meaning', db.Text, nullable=True)  
    fields = db.Column(db.ARRAY(db.String), nullable=True)  # Stores fields of study

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'inventors': self.inventors,
            'applicants': self.applicants,
            'publication_number': self.publication_number,
            'earliest_priority': self.earliest_priority.isoformat() if self.earliest_priority else None,
            'earliest_publication': self.earliest_publication.isoformat() if self.earliest_publication else None,
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
            'family_members': self.family_members,
            'first_publication_number': self.first_publication_number,
            'second_publication_number': self.second_publication_number,
            'first_publication_country': self.first_publication_country,
            'second_publication_country': self.second_publication_country,
            'ipc_meaning': self.ipc_meaning,
            'fields': self.fields
        }
        
class IPCFieldOfStudy(db.Model):
    __tablename__ = 'ipc_field_of_study'
    ipc = db.Column(db.String(20), primary_key=True)
    description = db.Column(db.Text, nullable=False)
    fields = db.Column(db.Text, nullable=False)

    def to_dict(self):
        return {
            'ipc': self.ipc_code,
            'description': self.description,
            'fields': self.fields        
        }
        
class Applicant(db.Model):
    __tablename__ = 'applicants'
    id = db.Column(db.Integer, primary_key=True)
    applicant_name = db.Column(db.Text, nullable=False)
    applicant_type = db.Column(db.String(100), nullable=False)

class ApplicantType(db.Model):
    __tablename__ = 'applicant_types'
    id = db.Column(db.Integer, primary_key=True)
    applicant_type = db.Column(db.String(100), nullable=False)
    percentage = db.Column(db.Float, nullable=False)