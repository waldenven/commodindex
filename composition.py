from pandas import DataFrame, Series, DateOffset
from pandas.core.datetools import BDay
from datetime import datetime
from tsdata.tsplotter import tsplotter

_roll5to9 = Series( map(lambda x: x/5.0,    range(1,6)),
            index = map(lambda x: x*BDay(), range(5,10)) )

def _t(d):
    return datetime.combine(d,datetime.min.time())
    
def LYY(d): return 'FGHJKMNQUVXZ'[d.month-1] + '%02d' % (d.year %100)
def L_monthnum(x): return 'FGHJKMNQUVXZ'.index(x) + 1

class composition:
    #roll_schedule          = 
    #year                   = 2000
    #month                  = 1
    #NC                     = 0
    #index_name             = 'GSCI'
    
    def __init__(self, n, y, m, rs = _roll5to9 ):
	self.index_name    = n
	self.year          = y
	self.month         = m
	self.roll_schedule = rs
	self.commodity_array_stored = DataFrame()
	self.NC            = 0
    
    def implied_name(self):
	return 'IC_%s_%04d_%02d' % (self.index_name, self.year, self.month)

    def first_reference_date(self):
	return datetime(self.year, self.month, 1 ).date()
	
    def reference_date(self, d):
	return d.replace(day=1)
	
    def first_roll_date(self):
	t = _t(self.first_reference_date()) - DateOffset() + self.roll_schedule.index[0]
	return t.date()
	
    def commodity_array(self):
	return self.commodity_array_stored
	
    def contracts(self, d, rori):
	assert rori in ['ro', 'ri']
	ca    = self.commodity_array()
	d_mon = LYY(d)[0]
	if rori == 'ri':
	    d_num = L_monthnum(d_mon)
	    d_mon = 'FGHJKMNQUVXZ'[ d_num % 12 ]
	sub   = {'LH' : 'LN' }
	cons  = map(lambda s, l: 'f_' + sub.get(s,s) + '_' + l + '%02d' % ( ( d.year + ( d.month > L_monthnum(l) ) ) % 100), ca['ticker'], ca[d_mon])
	return Series(cons, index=ca.index, name='contracts '+rori)
	    
    def roll_weights(self, d, rori):
	# should factor in MDE but does not now
	assert rori in ['ro', 'ri']
	idx = self.commodity_array().index
	if rori == 'ro':
	    rovals = map(lambda x: 1-x, self.roll_weights(d, 'ri'))
	    return Series( rovals, index=idx, name='weights '+rori)
	rs = self.roll_schedule.copy()
	rd = self.reference_date(d)
	dates = map( lambda d: (_t(rd) - DateOffset() + d).date(), rs.index)
	rs.index = dates
	#print d, rd, rs
	if d < rs.index[0]:
	    return Series( [0.0] * len(idx), index = idx, name='weights '+rori)
	idxrev = range(len(rs))
	idxrev.reverse()
	for n in idxrev:
	    if rs.index[n] <= d:
		return Series( [rs.ix[n]] * len(idx), index = idx, name='weights '+rori )
	return Series( [1.0] * len(idx), index = idx, name='weights '+rori )
	
class composition_curve:
    #index_name             = 'GSCI'
    #curve                  = Series([])
    
    def __init__(self, n, comps):
	self.index_name    = n
	self.curve         = Series( comps, index =
	                        map( lambda x: x.first_roll_date(), comps ) )
    
    def implied_name(self):
	return 'ICC_%s' % (self.index_name)
	
    def current_composition(self, d):
	c = self.curve
	ret = c.ix[0]
	for n in range(1, len(c)):
	    if c.index[n] > d:
		return ret
	    else:
		ret = c.ix[n]
	return ret

    def prior_date(self, d):
	ret = _t(d) - BDay() # Needs holiday cal too
	return ret.date()
	
    def contracts_prior(self, d, rori):
	pd     = self.prior_date(d)
	ic     = self.current_composition(pd)
	return ic.contracts(pd, rori)
	
    def roll_weights(self, d, rori):
	pd     = self.prior_date(d)
	ic     = self.current_composition(pd)
	return ic.roll_weights(pd, rori)
	
    def cpws_prior(self, d): # need to sort out rori
	pd     = self.prior_date(d)
	ic     = self.current_composition(pd)
	return ic.commodity_array()[['ticker', 'cpw']]
	
    def prices(self, d, cons, wts):
	#pd   = self.prior_date(d)
	#ic   = self.current_composition(d)
	#cons = ic.contracts(d, rori)
	#wts  = ic.roll_weights(d, rori)
	#nz   = filter(lambda w: w!= 0.0, wts)
	subidx = []
	toplot = []
	for k,w in wts.iteritems():
	    if w != 0.0:
		subidx.append( k )
		toplot.append( cons[k] )
	if len(toplot) == 0:
	    return Series([], name='prices')
	q = tsplotter( '\n'.join( toplot ))
	q.daterange = (d,d)  # really need minus 0b, and cannot be uniform
	curves = q.eval()
	return Series(map(lambda x: x.ix[0], curves), index=subidx, name='prices')
	
    def quotes(self, d, cons, wts):
	ret = self.prices(d, cons, wts)
	ret.name = 'quotes'
	return ret
	
    def return_computation(self, d):
	pd  = self.prior_date(d)
	#ic  = self.current_composition(d)
	df      = self.cpws_prior(d)
	cons_ro = self.contracts_prior(d, 'ro')
	cons_ri = self.contracts_prior(d, 'ri')
	wts_ro  = self.roll_weights(d, 'ro')
	wts_ri  = self.roll_weights(d, 'ri')
	
	df = df.join(cons_ro, how='outer')
	df = df.join(cons_ri, how='outer')
	df = df.join(wts_ro, how='outer')
	df = df.join(wts_ri, how='outer')
	
	pr_roy = self.prices(pd,cons_ro,wts_ro); pr_roy.name += ' roy'
	pr_riy = self.prices(pd,cons_ri,wts_ri); pr_riy.name += ' riy'
	pr_rot = self.prices(d,cons_ro,wts_ro); pr_rot.name += ' rot'
	pr_rit = self.prices(d,cons_ri,wts_ri); pr_rit.name += ' rit'
	df = df.join(pr_roy)
	df = df.join(pr_riy)
        df = df.join(pr_rot)
	df = df.join(pr_rit)
	#df = df.join(self.quotes(d, cons_ro,wts_ro))
	#df = df.join(self.quotes(d, cons_ri,wts_ri))
	
	return df
	
    #def contracts_ri_prior(self, d):
	#pd     = self.prior_date(d)
	#ic     = self.current_composition(pd)
	#return ic.contracts_ri(pd)
	
#rs           = Series( [ 0.2, 1 ], index = [ 5* BDay(), 9 * BDay() ] )
#IC_GSCI_2012 = composition( 'GSCI', 2012, 1 )
#IC_GSCI_2011 = composition( 'GSCI', 2011, 1 )
#comps        = [ IC_GSCI_2011, IC_GSCI_2012 ]
#c = Series( comps, index = map( lambda x: x.first_roll_date(), comps ) )
#IC_GSCI      = composition_curve( 'GSCI', c )


    #'commodity_array_stored' : {
        #'W'   :  18217.58,
        #'KW'   :  5004.071,
        #'C'   :  29648.15,
        #'S'   :  8037.317,
        #'KC'   :  17406.22,
        #'SB'   :  344724.8,
        #'CC'   :  4.116321,
        #'CT'   :  53411.21,
        #'LC'   :  92591.82,
        #'FC'   :  13596.46,
        #'LH'   :  72823.44,
        #'WTI'   :  13557.23,
        #'BRT'   :  6959.701,
        #'GO'   :  359.2745,
        #'HO'   :  71569.8,
        #'RB'   :  73694.1,
        #'NG'   :  28984.31,
        #'IA'   :  42.53,
        #'IC'   :  17.14,
        #'IL'   :  7.872,
        #'IN'   :  1.352,
        #'IZ'   :  11.04,
        #'GC'   :  76.58309,
        #'SI'   :  665.5205,
    #}
#}

#IC_GSCI_2011 = {
    #'commodity_array_stored' : {
        #'W'   :  18188.56,
        #'KW'   :  4134.2,
        #'C'   :  28210.87,
        #'S'   :  7708.699,
        #'KC'   :  16710,
        #'SB'   :  340773.4,
        #'CC'   :  4.015306,
        #'CT'   :  51632.55,
        #'LC'   :  91458.23,
        #'FC'   :  13417.1,
        #'LH'   :  70271.76,
        #'WTI'   :  14314,
        #'BRT'   :  6262.977,
        #'GO'   :  313.6761,
        #'HO'   :  72571.85,
        #'RB'   :  72504.78,
        #'NG'   :  28797.24,
        #'IA'   :  41.288,
        #'IC'   :  16.62,
        #'IL'   :  7.574,
        #'IN'   :  1.286,
        #'IZ'   :  10.68,
        #'GC'   :  78.12632,
        #'SI'   :  649.4452,
    #}
#}
