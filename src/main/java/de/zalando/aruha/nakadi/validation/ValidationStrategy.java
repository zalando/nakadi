package de.zalando.aruha.nakadi.validation;

import java.util.Map;

import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

public abstract class ValidationStrategy {

	private static final Map<String, ValidationStrategy> STRATEGIES = Maps.newHashMap();
	static {
		STRATEGIES.put(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
		STRATEGIES.put(FieldNameMustBeSet.NAME, new FieldNameMustBeSet());
	}

/*
	public JSONSchemaValidator materializeWithSchema(final JSONObject effectiveSchema) {
		return null;
	}
*/
	public static ValidationStrategy lookup(final String strategyName) {
		Preconditions.checkArgument(STRATEGIES.containsKey(strategyName), "No such strategy {}", strategyName);
		return STRATEGIES.get(strategyName);
	}
/*
	public boolean isValidFor(final JSONObject event) {
		// TODO Auto-generated method stub
		return false;
	}
*/

	public abstract EventValidator materialize(EventType eventType, ValidationStrategyConfiguration vsc);
}
