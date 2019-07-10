class InvalidParameterTypeException(Exception):
    """Used to raise an error of parameter type.
    """

    def __init__(self, p_message=None, p_parameter=None):
        """Create a new InvalidParameterTypeException instance.

            Args:
                p_message (str): the message related to the exception. Defaults to None.
                p_attribute (object): the parameter that caused the exception. Defaults to None.
        """

        super(InvalidParameterTypeException, self).__init__(
            '{p_message}\nReceived type: {p_type}.\nReceived value: {p_value}.'.format(
                p_message=p_message,
                p_type=type(p_parameter),
                p_value=p_parameter
            )
        )


class InvalidParameterValueException(Exception):
    """Used to raise an error of parameter value.
    """

    def __init__(self, p_message=None, p_parameter=None):
        """Create a new InvalidParameterValueException instance.

            Args:
                p_message (str): the message related to the exception. Defaults to None.
                p_attribute (object): the parameter that caused the exception. Defaults to None.
        """

        super(InvalidParameterValueException, self).__init__(
            '{p_message}\nReceived type: {p_type}.\nReceived value: {p_value}.'.format(
                p_message=p_message,
                p_type=type(p_parameter),
                p_value=p_parameter
            )
        )
