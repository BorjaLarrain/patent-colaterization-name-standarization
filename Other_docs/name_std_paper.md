

# C.2 Patenting by firms since 2006

The Google Patents data contain both granted patents and pending applications, but the
company producing the patent cannot be observed until the patent is granted, at which point
the inventor transfers it to the corporation. Thus, I restrict to patents that have actually
been granted by the Patent Office, and I manually match the company name to a list of
all NBER firm identifiers for US-headquartered companies that have been assigned at least
five patents as of year-end 2006 (this restriction is necessary to keep the processing time
manageable).

To match the company names, I take the first 30 characters of each name, eliminate
some of the most common corporate words, and then calculate a “distance” between pairs
of names, matching any pairs with a sufficiently low distance. My algorithm to calculate
this distance is as follows: First, I sequentially calculate the commonly-used Levenshtein
distance between each pair of words in the two names (that is, between the first two words,
the second two words, and so on). Next, I divide each word-pair distance by the length of
the longer of the two words, so that the penalty is per fraction, not number, of mismatched
letters. Finally, I divide again by the square of the word’s position in the company name,
so that the algorithm overweights the first few words of a company’s name. I add up the
resulting value from each word pair to get the overall distance between the two names.
Using this algorithm, I am able to match 59% of domestic corporate patents granted
since 2007 with firm identifiers (for comparison, 76% of domestic corporate patents granted
prior to 2007 are assigned firm identifiers in the NBER data), Extensive hand checking has
turned up no mistaken matches. Figure 11 displays the fraction of patents reported by the
USPTO that are accounted for in the NBER and in my data, dated by grant year in Panel (a)
and by application year in Panel (b). Citation pairs are also available for all granted patents
through the present, allowing me to construct the citation counts received by a patent within
any horizon of its grant date. Throughout the paper, I set this horizon to five years in an
attempt to mitigate right-truncation problems.


## Examples Figure 10:

> - Bank of America
> - Silicon Valley Bank
> - Wells Fargo
> - JPMorgan
> - Citi
> - General Electric Capital
> - Comerica
> - Credit Suisse
> - Bank of New York
> - Fleet
> - PNC Bank
> - Wilmington Trust
> - Deutsche Bank
> - US Bank
> - Wachovia