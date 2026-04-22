# SPDX-FileCopyrightText: 2026 Univention GmbH
# SPDX-License-Identifier: AGPL-3.0-only

class DN:  # TODO FIXME copied from univention-python (univention.DN)
    """
    A |LDAP| Distinguished Name.

    Shortened version of the DN class from univention-python.
    """

    _CASE_INSENSITIVE_ATTRIBUTES = {'cn', 'uid', 'dc', 'ou', 'c', 'l', 'o'}

    __slots__ = ('_dn', '_hash', '_str', 'dn')

    def __init__(self, dn: str) -> None:
        self.dn = dn
        self._hash = None
        self._str = None
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

    def __eq__(self, other: "DN") -> bool:
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

    def __ne__(self, other: "DN") -> bool:
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
