package net.m4.aws.glacier;

import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GlacierJobDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Jackson JSON mapper configuration.
 * 
 * @see
 * <a href="http://wiki.fasterxml.com/JacksonMixInAnnotations">http://wiki.fasterxml.com/JacksonMixInAnnotations</a>
 * <a href="http://wiki.fasterxml.com/JacksonFeatureModules">http://wiki.fasterxml.com/JacksonFeatureModules</a>
 *
 */
class AwsGlacierMixInModule extends SimpleModule {
	private static final long serialVersionUID = -7213534997633659786L;

	public AwsGlacierMixInModule() {
		super("VaultsTestModule", new Version(0, 0, 1, null, null, null));
	}

	@Override
	public void setupModule(SetupContext context) {
		context.setMixInAnnotations(GlacierJobDescription.class, GlacierJobDescriptionMixIn.class);
		context.setMixInAnnotations(DescribeJobResult.class, DescribeJobResultMixIn.class);
		// and other set up, if any
	}
}

/**
 * MixIn class for GlacierJobDescription
 *
 */
abstract class GlacierJobDescriptionMixIn extends GlacierJobDescription {
	private static final long serialVersionUID = -1866228620607371709L;

	@JsonIgnore
	public abstract Boolean getCompleted(); // we don't need it!

}

/**
 * MixIn class for DescribeJobResult
 *
 */
abstract class DescribeJobResultMixIn extends DescribeJobResult {
	private static final long serialVersionUID = 8577745463832177536L;

	@JsonIgnore
	public abstract Boolean getCompleted(); // we don't need it!
}