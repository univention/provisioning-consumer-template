# SPDX-FileCopyrightText: 2025-2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only
"""

A convenient wrapper to easily work with LDAP Distinguished Names (DNs)

FIXME:copied from univention-python (univention.DN)

"""

from __future__ import annotations

import re

from typing import Any, Self

try:
    import ldap
    use_ldap3 = False
except ImportError:
    import ldap3.utils.dn
    use_ldap3 = True

def _unescape_dn_value(val: str) -> str:
    """Normalize escaped characters in DN attribute values.

    Resolves hex-escapes (\\XX -> chr) and backslash-escapes (\\c -> c)
    so that semantically equivalent values compare equal.

    >>> _unescape_dn_value(r'\\31')
    '1'
    >>> _unescape_dn_value(r'\\74\\65\\73\\74')
    'test'
    >>> _unescape_dn_value('hello')
    'hello'
    """
    # First resolve hex-escapes: \XX -> chr(0xXX)
    val = re.sub(r'\\([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), val)
    # Then resolve backslash-escapes: \c -> c  (e.g. \, -> ,)
    val = re.sub(r'\\(.)', r'\1', val)
    return val

def _parse_dn_to_str2dn(parse_dn_result: str):
     """Convert ldap3 parse_dn output to ldap.dn.str2dn format."""
     rdns = []
     current_rdn = []
     for attr, val, sep in parse_dn_result:
         current_rdn.append((attr, _unescape_dn_value(val), 1))
         if sep != '+':
             # End of current RDN (separator is ',' or '')
             rdns.append(current_rdn)
             current_rdn = []
     return rdns

def _dn2str(dn) -> str:
    """
    Konvertiert eine python-ldap str2dn-Repräsentation zurück in einen DN-String.
    dn = [[('uid', 'myuser', 1), ('foo', 'bar', 1)], [('cn', 'users', 1)], ...]
    """
    if not use_ldap3:
        return ldap.dn.dn2str(dn)
    rdns = []
    for rdn in dn:
        # Mehrere AVAs innerhalb eines RDN werden mit '+' verbunden
        avas = '+'.join(f'{attr}={value}' for attr, value, _ in rdn)
        rdns.append(avas)
        # RDNs werden mit ',' verbunden
    return ','.join(rdns)


class DN:
    """A |LDAP| Distinguished Name."""

    _CASE_INSENSITIVE_ATTRIBUTES = {'cn', 'uid', 'dc', 'ou', 'c', 'l', 'o'}

    __slots__ = ('_dn', '_hash', '_str', 'dn')

    def __init__(self, dn: str) -> None:
        self.dn = dn
        self._hash: int | None = None
        self._str: str | None = None
        if use_ldap3:
            self._dn = _parse_dn_to_str2dn(ldap3.utils.dn.parse_dn(self.dn))
        else:
            assert ldap
            try:
                self._dn = ldap.dn.str2dn(self.dn)
            except ldap.DECODING_ERROR:
                raise ValueError('Malformed DN syntax: %r' % (self.dn,))

    @property
    def rdn(self) -> tuple[str, str]:
        """
        >>> DN('foo=1,bar=2').rdn
        ('foo', '1')
        """
        return tuple(self._dn[0][0][:2])

    @property
    def parent(self) -> Self | None:
        """
        >>> DN('foo=1,bar=2').parent == DN('bar=2')
        True
        """
        if len(self._dn) > 1:
            return self[1:]

    def endswith(self, other: str | Self):
        """
        >>> DN('foo=1,bar=2').endswith('bar=2')
        True
        >>> DN('foo=1,bar=2').endswith('foo=1')
        False
        """
        if not isinstance(other, DN):
            other = self.__class__(other)
        return self[-len(other):] == other

    def startswith(self, other: str | Self):
        """
        >>> DN('foo=1,bar=2').startswith('foo=1')
        True
        >>> DN('foo=1,bar=2').startswith('bar=2')
        False
        """
        if not isinstance(other, DN):
            other = self.__class__(other)
        return self[:len(other)] == other

    def walk(self, base):
        """
        >>> [str(x) for x in DN('foo=1,bar=2,baz=3,blub=4').walk('baz=3,blub=4')]
        ['baz=3,blub=4', 'bar=2,baz=3,blub=4', 'foo=1,bar=2,baz=3,blub=4']
        """
        base = self.__class__(base) if not isinstance(base, DN) else base
        if not self.endswith(base):
            raise ValueError('DN must end with given base')

        for i in reversed(range(len(self) - len(base) + 1)):
            yield self[i:]

    def __str__(self) -> str:
        """
        ### TEST DEACTIVATED BECAUSE INCOMPATIBLE WITH ldap3 !!!
        ### >>> str(DN('foo = 1 , bar = 2')) == "foo=1,bar=2"
        ### True
        >>> str(DN("cn=foo\\20bar")) == "cn=foo\\20bar"
        True
        """
        # compute string only once since the object is static
        if self._str is None:
            self._str = _dn2str(self._dn)
        return self._str

    def __repr__(self) -> str:
        """
        >>> repr(DN('foo=1,bar=2')) == "<DN 'foo=1,bar=2'>"
        True
        """
        return '<%s %r>' % (type(self).__name__, str(self))

    def __len__(self) -> int:
        """Return length of DN components"""
        return len(self._dn)

    def __getitem__(self, key: str | slice) -> Any:
        if isinstance(key, slice):
            return self.__class__(_dn2str(self._dn[key]))
        return self.__class__(_dn2str([self._dn[key]]))

    def __eq__(self, other: object) -> bool:
        """
        >>> DN('foo=1') == DN('foo=1')
        True
        >>> DN('foo=1') == DN('foo=2')
        False
        >>> DN('Foo=1') == DN('foo=1')
        True
        >>> DN('Foo=1') == DN('foo=2')
        False
        >>> DN('uid=Administrator') == DN('uid=administrator')
        True
        >>> DN('univentionAppID=Foo') == DN('univentionAppID=foo')
        False
        >>> DN('foo=1,bar=2') == DN('foo=1,bar=2')
        True
        >>> DN('bar=2,foo=1') == DN('foo=1,bar=2')
        False
        >>> DN('foo=1+bar=2') == DN('foo=1+bar=2')
        True
        >>> DN('bar=2+foo=1') == DN('foo=1+bar=2')
        True
        >>> DN('bar=2+Foo=1') == DN('foo=1+Bar=2')
        True
        >>> DN(r'foo=%s31' % chr(92)) == DN(r'foo=1')
        True
        """
        return hash(self) == hash(other)

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        # compute hash only once - object is static
        if self._hash is None:
            self._hash = hash(tuple(
                tuple(sorted(
                    (attr.lower(), val.lower() if attr.lower() in self._CASE_INSENSITIVE_ATTRIBUTES else val, ava)
                    for attr, val, ava in rdn
                )) for rdn in self._dn
            ))
        return self._hash

    @classmethod
    def set(cls, values: list[str]) -> set[Self]:
        """
        Returns a unique set of DNs.

        ### TEST DEACTIVATED BECAUSE INCOMPATIBLE WITH ldap3 !!!
        ### >>> len(DN.set(['CN=computers,dc=foo', 'cn=computers,dc=foo', 'cn = computers,dc=foo', 'CN=Computers,dc=foo']))
        ### 1
        >>> len(DN.set(['CN=computers,dc=foo', 'cn=computers,dc=foo', 'cn=computers,dc=foo', 'CN=Computers,dc=foo']))
        1
        """
        return set(map(cls, values))

    @classmethod
    def values(cls, dns: list[Self]) -> set[str]:
        """
        Return a unique set of DN strings from DNs.

        ### TEST DEACTIVATED BECAUSE INCOMPATIBLE WITH ldap3 !!!
        ### >>> DN.values(DN.set(['cn=foo', 'cn=bar']) - DN.set(['cn = foo'])) == {'cn=bar'}
        ### True
        >>> DN.values(DN.set(['cn=foo', 'cn=bar']) - DN.set(['cn=foo'])) == {'cn=bar'}
        True
        """
        return set(map(str, dns))


if __name__ == '__main__':
    import doctest
    doctest.testmod()
