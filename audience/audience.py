import pprint

from . import config 
from . import models

from facebookads import FacebookAdsApi
from facebookads.session import FacebookSession
from facebookads.objects import (AdAccount, CustomAudience)
from datetime import datetime, date


class Sorter:
    """ The <[Sorter Object]> connects to a specified sqlite database
	and table containing customer data. It has several methods for categorizing
	customer records into specific buckets.

	There are type types of customer sorts.

	An 'add_sort' sorts members into add categories.
	
	A 'add_remove_sort' sorts members into remove and add categories.
	
	Lastly, this object follows the Exclusive Or (XOR) logic model when
	it comes to classifying customers:
	
	A v B & ~(A & B)
	
	'You can either choose to perform an add_sort or an
	add_remove_sort but not both.'

    :params table: str, table you want to use for data source
	
	return :: container.Sorter object
	"""
    
    def __init__(self, table=None):
        try:
            self.__customers = models.__dict__[table]
        except KeyError:
            self.__customers = models.customers
        
        self._ca, self._la, self._ela = [], [], []  # ADD user lists
        self._cad, self._lad, self._elad = [], [], []  # DELETE user lists
    
    def _generate_deletes(self):
        """ This builds a list of dictionaries containing
        the users that need to be removed and added for certain
        audiences.
        
        There are 3 buckets based on difference of days between
        report run time and last order date:

            ::::> [0,90]
            ::::> (90,730]
            ::::> [730, inf)
        
        We find the deletes by looking at the difference between today's date
        and the last order date. If it falls within a certain bucket and its
        segment does not match that bucket, we move it to the next bucket, add 
        as delete in the current bucket and rename the record's segment. 
        """
        self._la, self._ela = [], []
        self._cad, self._lad, self._elad = [], [], [] 
        
        today = date.today()
        
        for record in self.__customers.select():
            order = datetime.strptime(record.last_order_date, '%Y-%m-%d').date()
            if (today-order).days > 90 and (today-order).days <= 730:
                if record.segment == 'current':
                    self._cad.append(record.usa_email)
                    self._la.append(record.usa_email)
                    record.segment = 'lapsed'
                    record.save()
            if (today-order).days > 730:
                if record.segment == 'lapsed':
                    self._lad.append(record.usa_email)
                    self._ela.append(record.usa_email)
                    record.segment = 'extra lapsed'
                    record.save()

    def _generate_pushes(self, initial=False):
        """ _generates_pushes builds list of dictionaries containing the users that
        need to be pushed to specific audience endpoints.
        
        One new file is added weekly and that new file contains the 
        customers with the latest order dates. None of the other files 
        are updated. Thus to get the import or "push" customers, you 
        need to grab the records that came from the most recent file.
        This is determined by the file_parse_date field in the database 
        table.
        
        :params initial: boolean, True = initial sort; False = continous sort
        """
        self._ca, self._la, self._ela = [], [], []
        table = self.__customers
        
        if initial:
            for record in table.select():
                if record.segment == 'current':
                    self._ca.append(record.usa_email)
                if record.segment == 'lapsed':
                    self._la.append(record.usa_email)
                if record.segment == 'extra lapsed':
                    self._ela.append(record.usa_email)
        else:
            target_field = table.file_parse_date
            check = table.select().order_by(target_field.desc()).get()
            print('file parse date: {}'.format(check.file_parse_date))
            for record in table.select().where(target_field == check.file_parse_date):
                self._ca.append(record.usa_email)
    
    @property
    def add_sort(self):
        self._generate_pushes(initial=True)
        return print("Add Sorting Complete.")
    
    @property
    def add_remove_sort(self):
        self._generate_pushes()
        self._generate_deletes()
        return print("Add-Remove Sorting Complete.")
    
    @property
    def current(self):
        if hasattr(self, '_ca'):
            return self._ca
        else:
        	return False
    
    @property
    def lapsed(self):
        if hasattr(self, '_la'):
        	return self._la
        else:
        	return False
    
    @property
    def extra_lapsed(self):
        if hasattr(self, '_ela'):
    	    return self._ela
        else:
    	    return False
    
    @property
    def current_deletes(self):
        if hasattr(self, '_cad'):
    	    return self._cad
        else:
    	    return False
    
    @property
    def lapsed_deletes(self):
        if hasattr(self, '_lad'):
    	    return self._lad
        else:
    	    return False
    
    @property
    def extra_lapsed_deletes(self):
        if hasattr(self, '_elad'):
    	    return self._elad
        else:
    	    return False
    
    def __str__(self):
        return '<[Sorter Object]>'
    
    def __repr__(self):
        return '<Sorter Object [{}]>'.format(config.DATABASE_PATH)


class Adapter:
    """ An object designed to make managing custom audiences a little easier.
    
    :params account: str, account id
    :params table: str, table name from database
    
    return :: container.Adapter object
    """
    __session = FacebookSession(config.APP_ID, config.APP_SECRET,
			 config.ACCESS_TOKEN)
    __api = FacebookAdsApi(__session)
    
    def __init__(self, account=None, table='customers'):
        FacebookAdsApi.set_default_api(self.__api)
        if account:
            self._account = 'act_{}'.format(account)
            self.__api.set_default_account_id = self._account
            self._audiences = AdAccount(self._account).get_custom_audiences(
                fields=[CustomAudience.Field.name, CustomAudience.Field.id])
            self._responses = []
    
    def _get_audience(self, audience_name):
        """ This retrieves an audience object based on a string name.
        
        :params audience_name: str, name of audience
        """
        for audience in list(self.audiences):
            if audience['name'] == audience_name:
                audience_id = audience['id']
        
        target = CustomAudience(audience_id)
        
        return target
    
    def _batch_users(self, obj, size=2500):
        """ This returns a generator that returns a list 
        of lists of a specific size.
        
        :params obj: list, users list that needs to be batched
        :params size: int, the batch size
        """
        for chunk in range(0, len(obj), size):
            try:
                yield obj[chunk:chunk + size]
            except IndexError:
                yield obj[chunk:len(obj)]
    
    def create_audience(self, name, desc=None):
        """ This creates an audience object.
        
        :params name: str, name of audience
        :params desc: str, description of audience
        """
        if name in [audience['name'] for audience in self.audiences]:
            raise ValueError('Attempted to add audience. Audience with same name exists.')
        
        audience = CustomAudience(parent_id=self._account)
        audience[CustomAudience.Field.subtype] = CustomAudience.Subtype.custom
        audience[CustomAudience.Field.name] = '{}'.format(name)
        
        if desc:
            audience[CustomAudience.Field.description] = desc
        audience.remote_create()
    
    def delete_audience(self, name):
        """ This deletes an audience object.
        
        :params name: str, name of audience
        """
        if name not in [audience['name'] for audience in self.audiences]:
            raise ValueError('Attempted to remove audience. Audience does not exist.')
        
        for audience in list(self.audiences):
            if audience['name'] == name:
                delete_id = audience['id']
        
        audience = CustomAudience(delete_id)
        audience.remote_delete()
    
    def add_users(self, name, users):
        """ This bulk adds users to an audience object.
        
        :params name: str, name of audience
        :params users: list, list of users
        """
        if not len(users):
            return print('Attempted to add users. No users in the list.')
        
        print('Adding {} users to {}'.format(len(users), name))
        
        if not isinstance(users, list):
            raise TypeError
        
        target = self._get_audience(name)
        
        if len(users) > 10000:  # User add limit is ~10000.
            batches = self._batch_users(users)
            for batch in batches:
                post_ = target.add_users(CustomAudience.Schema.email_hash, batch, is_raw=True)
                pprint.pprint(post_._body)
        else:
            post_ = target.add_users(CustomAudience.Schema.email_hash, users, is_raw=True)
        
    def remove_users(self, name, users):
        """ This bulk deletes users from an audience object.
        
        :params name: str, name of audience
        :params users: list, list of users
        """
        if not len(users):
            return print('Attempted to remove users. No users in the list.')
        
        print('Removing {} users to {}'.format(len(users), name))
        
        if not isinstance(users, list):
            raise TypeError
        
        target = self._get_audience(name)
        
        if len(users) > 500:  # User delete limit is 500 < x < 1000.
            batches = self._batch_users(users, size=500)
            for batch in batches:
                target.remove_users(CustomAudience.Schema.email_hash, batch)
        else:
            target.remove_users(CustomAudience.Schema.email_hash, users)
    
    @property
    def audiences(self):
        return AdAccount(self._account).get_custom_audiences(
            fields=[CustomAudience.Field.name, CustomAudience.Field.id])
    
    def __str__(self):
        return '<[Adapter Object]>'
    
    def __repr__(self):
        return '<Adapter Object [{}]>'.form(self._account)
