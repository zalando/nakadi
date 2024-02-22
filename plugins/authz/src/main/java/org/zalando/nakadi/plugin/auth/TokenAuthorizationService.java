package org.zalando.nakadi.plugin.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.attribute.TeamAuthorizationAttribute;
import org.zalando.nakadi.plugin.auth.subject.Principal;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.zalando.nakadi.plugin.auth.ResourceType.ALL_DATA_ACCESS_RESOURCE;
import static org.zalando.nakadi.plugin.auth.ResourceType.EVENT_TYPE_RESOURCE;
import static org.zalando.nakadi.plugin.auth.ResourceType.PERMISSION_RESOURCE;
import static org.zalando.nakadi.plugin.auth.ResourceType.SUBSCRIPTION_RESOURCE;

public class TokenAuthorizationService implements AuthorizationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenAuthorizationService.class);

    public static final String SERVICE_PREFIX = "stups_";
    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final String usersType;
    private final String servicesType;
    private final String businessPartnersType;

    private final List<String> merchantUids;

    private static final Map<String, List<String>> BUSINESS_PARTNER_ALLOWED_OPERATION = new HashMap<>() {{
        put(SUBSCRIPTION_RESOURCE, Arrays.asList(Operation.READ.toString(), Operation.ADMIN.toString()));
        put(EVENT_TYPE_RESOURCE, Arrays.asList(Operation.READ.toString()));
    }};

    private final ValueRegistry applicatoinRegistry;
    private final ValueRegistry merchantRegistry;
    private final ValueRegistry userRegistry;
    private final ZalandoTeamService teamService;

    public TokenAuthorizationService(final String usersType,
                                     final ValueRegistry userRegistry,
                                     final String servicesType,
                                     final ValueRegistry applicationRegistry,
                                     final String businessPartnersType,
                                     final ValueRegistry merchantRegistry,
                                     final ZalandoTeamService teamService,
                                     final List<String> merchantUids) {
        this.usersType = usersType;
        this.servicesType = servicesType;
        this.businessPartnersType = businessPartnersType;
        this.merchantUids = merchantUids;
        this.applicatoinRegistry = applicationRegistry;
        this.merchantRegistry = merchantRegistry;
        this.teamService = teamService;
        this.userRegistry = userRegistry;
    }

    @Override
    public boolean isAuthorized(final Operation operation, final Resource resource)
            throws PluginException {
        return getPrincipal(true)
                .isAuthorized(resource.getType(), operation, resource.getAttributesForOperation(operation));
    }

    private Principal getPrincipal(final boolean throwOnError) {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            if (throwOnError) {
                throw new PluginException("No Authentication found in security context");
            } else {
                return null;
            }
        }
        if (!(authentication instanceof OAuth2Authentication)) {
            if (throwOnError) {
                throw new PluginException("Authentication object is not of type OAuth2Authentication, but "
                        + authentication.getClass().getCanonicalName());
            } else {
                return null;
            }
        }
        final OAuth2Authentication castedAuthentication = (OAuth2Authentication) authentication;
        return (Principal) castedAuthentication.getPrincipal();
    }

    @Override
    public void isAuthorizationForResourceValid(final Resource resource) throws AuthorizationInvalidException {
        final Principal subject = getPrincipal(true);
        //For BP with empty authorization
        if (resource.getAuthorization() == null) {
            if (subject.isExternal()) {
                throw new AuthorizationInvalidException("Empty authorization is not allowed");
            } else {
                //For Zalando Employee with Empty authorization
                return;
            }
        }

        final Map<String, List<AuthorizationAttribute>> authorization = resource.getAuthorization();

        isAuthorizationAttributeValid(authorization);
        if (isGatewayInAuthorization(authorization)) {
            throw new AuthorizationInvalidException("Gateway is not allowed in authorization section");
        }
        if (!isBPPresentInAuthorization(authorization)) {
            if (!subject.isExternal()) {           //for Zalando Employees
                return;
            } else {
                throw new AuthorizationInvalidException("Authorization should contain business partner");
            }
        }
        //For BPs present in Authorization
        if (resource.getType().equals(PERMISSION_RESOURCE)) {
            throw new OperationOnResourceNotPermittedException
                    ("Business Partner is not allowed access to the resource");
        }
        if (resource.getType().equals(ALL_DATA_ACCESS_RESOURCE)) {
            throw new OperationOnResourceNotPermittedException
                    ("Business Partner is not allowed access to the resource");
        }
        if (resource.getType().equals(SUBSCRIPTION_RESOURCE)) {
            if (!subject.isExternal()) {
                throw new OperationOnResourceNotPermittedException
                        ("Subscription including business partner can " +
                                "be only created by corresponding business partner");
            }
            if (!isBPInAdminAndReader(subject.getBpids(), authorization)) {
                throw new AuthorizationInvalidException
                        ("Business partner must only add itself as both admin and reader");
            }
        }
        if (resource.getType().equals(EVENT_TYPE_RESOURCE)) {
            if (!isEventTypeNonLogCompactedAndCompatible(resource)) {
                throw new AuthorizationInvalidException
                        ("Event Type must be non-log compacted and compatible");
            }
            if (!isOnlyOneBPForEachOperation(authorization)) {
                throw new AuthorizationInvalidException
                        ("Only one business partner allowed for each operation");
            }
        }
        if (isAnyWildCardPresent(authorization)) {
            throw new AuthorizationInvalidException
                    ("Business Partner cannot be present with wild card in authorization");
        }
        if (!isBPWithOperationAuthorized(resource.getType(), authorization)) {
            throw new OperationOnResourceNotPermittedException
                    ("Business Partner is not allowed to perform the operation");
        }

    }

    private boolean isEventTypeNonLogCompactedAndCompatible(final Resource resource) throws PluginException {
        if (!(resource.get() instanceof EventTypeAuthz)) {
            throw new PluginException("Resource is not EventType");
        }
        final EventTypeAuthz event = (EventTypeAuthz) resource.get();
        return event.getAuthCleanupPolicy().equalsIgnoreCase("DELETE")
                && event.getAuthCompatibilityMode().equalsIgnoreCase("COMPATIBLE");

    }

    private boolean isBPPresentInAuthorization
            (final Map<String, List<AuthorizationAttribute>> authorizationAttributes) {
        return authorizationAttributes.values().stream().anyMatch(attributeList -> attributeList.stream()
                .map(AuthorizationAttribute::getDataType).anyMatch(a -> a.equals(businessPartnersType)));
    }

    private boolean isOnlyOneBPForEachOperation
            (final Map<String, List<AuthorizationAttribute>> authorizationAttributes) {
        return authorizationAttributes.values().stream().allMatch(
                attributeList -> attributeList.stream().map(AuthorizationAttribute::getDataType)
                        .filter(a -> a.equals(businessPartnersType)).count() <= 1);
    }

    private boolean isBPInAdminAndReader(final Set<String> bpIds,
                                         final Map<String, List<AuthorizationAttribute>> authorizationAttributes) {

        if (!isOnlyOneBPForEachOperation(authorizationAttributes)) {
            return false;
        }

        return authorizationAttributes.entrySet().stream().allMatch(k -> {
            if (k.getKey().equals(Operation.ADMIN.toString()) || k.getKey().equals(Operation.READ.toString())) {
                return k.getValue().stream().map(AuthorizationAttribute::getValue).allMatch(bpIds::contains);
            } else {
                return false;
            }
        });

    }

    private boolean isAnyWildCardPresent(final Map<String, List<AuthorizationAttribute>> authorizationAttributes) {
        return authorizationAttributes.values().stream().anyMatch(attributeList ->
                attributeList.stream().anyMatch(a -> a.getDataType().equals("*") || a.getValue().equals("*")));

    }

    private void isAuthorizationAttributeValid(
            final Map<String, List<AuthorizationAttribute>> authorizationAttributes)
            throws AuthorizationInvalidException {

        final List<String> errors = new LinkedList<>();
        for (final Map.Entry<String, List<AuthorizationAttribute>> entry : authorizationAttributes.entrySet()) {
            entry.getValue().forEach(attr -> {
                if (!isAuthorizationAttributeValidInternal(entry.getKey(), attr)) {
                    errors.add(String.format("authorization attribute %s:%s is invalid",
                            attr.getDataType(), attr.getValue()));
                }
            });
        }

        if (!errors.isEmpty()) {
            final String errorMessage = errors.stream().collect(Collectors.joining(", "));
            throw new AuthorizationInvalidException(errorMessage);
        }
    }

    private boolean isGatewayInAuthorization(final Map<String, List<AuthorizationAttribute>> authorizationAttributes) {
        return authorizationAttributes.values().stream().flatMap(Collection::stream).anyMatch(this::isGateway);
    }

    private boolean isGateway(final AuthorizationAttribute attr) {
        return attr.getDataType().equals(servicesType) && merchantUids.contains(attr.getValue());
    }

    private boolean isAuthorizationAttributeValidInternal(final String operation,
                                                          final AuthorizationAttribute authorizationAttribute)
            throws PluginException {

        if (authorizationAttribute.getDataType().equals("*") &&
                authorizationAttribute.getValue().equals("*")) {
            return !Operation.ADMIN.toString().equals(operation);
        }
        if (authorizationAttribute.getDataType().equals(servicesType)) {
            final String serviceName = authorizationAttribute.getValue();
            if (serviceName.startsWith(SERVICE_PREFIX)) {
                return applicatoinRegistry.isValid(serviceName.substring(SERVICE_PREFIX.length()));
            } else {
                // A UUID could be one of the statically created clients used by eg. Lounge.
                // Cannot be validated, so we have to hope that the user who adds this doesn't make a typo.
                // See https://groups.google.com/a/zalando.de/forum/#!topic/iam-support/Z8gp9PkTr7I
                // if it's not a UUID then it's definitely not valid.
                return UUID_PATTERN.matcher(serviceName).matches();
            }
        }
        if (authorizationAttribute.getDataType().equals(businessPartnersType)) {
            return merchantRegistry.isValid(authorizationAttribute.getValue());
        }
        if (authorizationAttribute.getDataType().equals(usersType)) {
            return userRegistry.isValid(authorizationAttribute.getValue());
        }
        if (TeamAuthorizationAttribute.isTeamAuthorizationAttribute(authorizationAttribute)) {
            return teamService.isValidTeam(authorizationAttribute.getValue());
        }

        LOGGER.error("Could not validate authorization attribute: " + authorizationAttribute);
        return false;
    }


    private boolean isBPWithOperationAuthorized(final String resourceType,
                                                final Map<String,
                                                        List<AuthorizationAttribute>> authorizationAttributes) {

        if (!BUSINESS_PARTNER_ALLOWED_OPERATION.containsKey(resourceType)) {
            return false;
        }

        return authorizationAttributes.entrySet().stream()
                .filter(k -> k.getValue().stream().map(AuthorizationAttribute::getDataType)
                        .anyMatch(a -> a.equals(businessPartnersType)))
                .map(Map.Entry::getKey)
                .allMatch(operation -> BUSINESS_PARTNER_ALLOWED_OPERATION.get(resourceType)
                        .contains(operation));

    }

    @Override
    public List<Resource> filter(final List<Resource> input) {
        return input;
    }


    @Override
    public Optional<Subject> getSubject() throws PluginException {
        return Optional.ofNullable(getPrincipal(false));
    }
}
