# openlxp-xia-dau
The DAU Experience Index Agent (XIA) component for the OpenLXP solution.

# Workflows
The DAU XIA implements five core workflows, as follows:

Extract: Pulls pertinent learning experience metadata records from the corresponding Experience Source Repository (XSR).

Validate: Compares extracted learning experience metadata against the configured source metadata reference schema stored in the Experience Schema Service (XSS).

Transform: Transforms extracted+validated source learning experience metadata to the configured target schema using the "XSR-to-Target" transformation map stored in the Experience Schema Service (XSS)

Validate: Compares transformed learning experience metadata against the configured target metadata reference schema stored in the Experience Schema Service (XSS).

Load: Pushes transformed and validated learning experience metadata to the target Experience Index Service (XIS) for further processing.

Log: Records error, warning, informational, and debug events which can be reviewed and monitored.
