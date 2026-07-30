package daemon

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"

	"github.com/meza/RunnerFoundry/internal/config"
)

type postgresCredentialFixture struct {
	missingReference string
}

func newPostgresCredentialFixture() postgresCredentialFixture {
	return postgresCredentialFixture{missingReference: "env://RUNNERFOUNDRY_MISSING_POSTGRES_PASSWORD"}
}

func testAppCredentials(t *testing.T) config.GitHubAppCredentials {
	t.Helper()
	primeOne, validOne := new(big.Int).SetString("C49B48187124EE531F26C48AEBB4E3FD4C069D73DD3DA35D0AA87F46834A9BF3F2D3CD90BEDCB8199DBD2552CE670106A95ED0A3E0AE559FA68E846339D542CB", 16)
	primeTwo, validTwo := new(big.Int).SetString("F24E7544C243EC2DC2D3CD84F1B7CB1561FD7A25FE50D139AA338957EC2C5C64D68D8927518766C38CF2A0EA5B17F7D4C784B75B33F0316A0D82248F24533DF7", 16)
	if !validOne || !validTwo {
		t.Fatal("invalid deterministic RSA fixture")
	}
	one := big.NewInt(1)
	modulus := new(big.Int).Mul(primeOne, primeTwo)
	totient := new(big.Int).Mul(new(big.Int).Sub(primeOne, one), new(big.Int).Sub(primeTwo, one))
	privateKey := &rsa.PrivateKey{PublicKey: rsa.PublicKey{N: modulus, E: 65537},
		D: new(big.Int).ModInverse(big.NewInt(65537), totient), Primes: []*big.Int{primeOne, primeTwo}}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return config.GitHubAppCredentials{AppID: 1, PrivateKey: string(privateKeyPEM)}
}
