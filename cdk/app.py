from aws_cdk import App

from settings import StackSettings
from stack import HlsViStack


settings = StackSettings()

app = App()
stack = HlsViStack(
    app,
    settings.STACK_NAME,
    settings,
    env={"account": settings.MCP_ACCOUNT_ID, "region": settings.MCP_ACCOUNT_REGION},
)

app.synth()
