package flagutil

import (
	"fmt"
	"os"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	flag "github.com/spf13/pflag"
)

var _ = Describe("Env", func() {
	var fs *flag.FlagSet
	var strVal *string
	var boolVal *bool
	var intVal *int
	var prefix string

	BeforeEach(func() {
		fs = flag.NewFlagSet("test", flag.ContinueOnError)
		strVal = fs.String("test-string", "default_str", "A test string flag")
		boolVal = fs.Bool("test-bool", false, "A test boolean flag")
		intVal = fs.Int("test-int", 0, "A test integer flag")
		prefix = "TEST_APP"

		// Clear all relevant environment variables before each test
		os.Unsetenv(prefix + "_TEST_STRING")
		os.Unsetenv(prefix + "_TEST_BOOL")
		os.Unsetenv(prefix + "_TEST_INT")
	})

	Context("when environment variables are present and flags are unset", func() {
		It("should set string flag from environment variable", func() {
			os.Setenv(prefix+"_TEST_STRING", "env_str_value")
			defer os.Unsetenv(prefix + "_TEST_STRING")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*strVal).To(Equal("env_str_value"))
		})

		It("should set boolean flag from environment variable", func() {
			os.Setenv(prefix+"_TEST_BOOL", "true")
			defer os.Unsetenv(prefix + "_TEST_BOOL")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*boolVal).To(BeTrue())
		})

		It("should set integer flag from environment variable", func() {
			os.Setenv(prefix+"_TEST_INT", "123")
			defer os.Unsetenv(prefix + "_TEST_INT")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*intVal).To(Equal(123))
		})
	})

	Context("when flags are already set", func() {
		It("should not override an already set string flag", func() {
			fs.Set("test-string", "explicit_str_value") // Set the flag explicitly
			os.Setenv(prefix+"_TEST_STRING", "env_str_value")
			defer os.Unsetenv(prefix + "_TEST_STRING")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*strVal).To(Equal("explicit_str_value")) // Should retain explicit value
		})

		It("should not override an already set boolean flag", func() {
			fs.Set("test-bool", "true")
			os.Setenv(prefix+"_TEST_BOOL", "false")
			defer os.Unsetenv(prefix + "_TEST_BOOL")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*boolVal).To(BeTrue())
		})

		It("should not override an already set integer flag", func() {
			fs.Set("test-int", "999")
			os.Setenv(prefix+"_TEST_INT", "111")
			defer os.Unsetenv(prefix + "_TEST_INT")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*intVal).To(Equal(999))
		})
	})

	Context("when environment variables are not present", func() {
		It("should not change the default value of string flag", func() {
			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*strVal).To(Equal("default_str")) // Should retain default value
		})

		It("should not change the default value of boolean flag", func() {
			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*boolVal).To(BeFalse()) // Should retain default value
		})

		It("should not change the default value of integer flag", func() {
			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*intVal).To(Equal(0)) // Should retain default value
		})
	})

	Context("with invalid environment variable values", func() {
		It("should return an error for invalid boolean value", func() {
			os.Setenv(prefix+"_TEST_BOOL", "not-a-bool")
			defer os.Unsetenv(prefix + "_TEST_BOOL")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid value"))
		})

		It("should return an error for invalid integer value", func() {
			os.Setenv(prefix+"_TEST_INT", "not-an-int")
			defer os.Unsetenv(prefix + "_TEST_INT")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid value"))
		})
	})

	Context("with a different prefix", func() {
		It("should correctly set a flag with a custom prefix", func() {
			customPrefix := "CUSTOM_APP"
			fs2 := flag.NewFlagSet("custom", flag.ContinueOnError)
			customStrVal := fs2.String("custom-flag", "def", "A custom flag")

			os.Setenv(customPrefix+"_CUSTOM_FLAG", "custom_env_value")
			defer os.Unsetenv(customPrefix + "_CUSTOM_FLAG")

			err := SetFlagsFromEnv(fs2, customPrefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*customStrVal).To(Equal("custom_env_value"))
		})
	})

	Context("with flag name casing and dashes", func() {
		var camelCaseFlag *string
		var noDashFlag *string
		var multipleDashFlag *string

		BeforeEach(func() {
			camelCaseFlag = fs.String("camelCaseFlag", "default", "Camel case flag")
			noDashFlag = fs.String("nodashflag", "default", "No dash flag")
			multipleDashFlag = fs.String("multiple-dash-flag", "default", "Multiple dash flag")

			// Unset environment variables for these flags
			os.Unsetenv(prefix + "_CAMELCASEFLAG")
			os.Unsetenv(prefix + "_NODASHFLAG")
			os.Unsetenv(prefix + "_MULTIPLE_DASH_FLAG")
		})

		It("should correctly map camelCaseFlag to UPPERCASE environment variable", func() {
			os.Setenv(prefix+"_CAMELCASEFLAG", "camel_env")
			defer os.Unsetenv(prefix + "_CAMELCASEFLAG")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*camelCaseFlag).To(Equal("camel_env"))
		})

		It("should correctly map no-dash flag to UPPERCASE environment variable", func() {
			os.Setenv(prefix+"_NODASHFLAG", "nodash_env")
			defer os.Unsetenv(prefix + "_NODASHFLAG")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*noDashFlag).To(Equal("nodash_env"))
		})

		It("should correctly map multiple-dash-flag to UPPERCASE_UNDERSCORE environment variable", func() {
			os.Setenv(prefix+"_MULTIPLE_DASH_FLAG", "multi_dash_env")
			defer os.Unsetenv(prefix + "_MULTIPLE_DASH_FLAG")

			err := SetFlagsFromEnv(fs, prefix)
			Expect(err).ToNot(HaveOccurred())
			Expect(*multipleDashFlag).To(Equal("multi_dash_env"))
		})
	})
})

// Helper to convert string to int, needed for explicit `fs.Set` for int flags,
// as `fs.Set` expects string and handles conversion, but we need to ensure test setup is correct.
func mustInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("Failed to convert %q to int: %v", s, err))
	}
	return i
}
