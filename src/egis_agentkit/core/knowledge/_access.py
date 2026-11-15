"""按已核实的用户和启用组计算知识库范围；应用身份与管理员角色不参与推断。"""


def resolve_knowledge_scope(user_id: str, group_ids: set[str], grants) -> set[str]:
    """仅合并显式个人/组授权，不把公司、团队或应用的范围当成隐式权限。"""
    return {
        kb_id
        for grant in grants
        if (grant.principal_type == "user" and grant.principal_id == user_id)
        or (grant.principal_type == "group" and grant.principal_id in group_ids)
        for kb_id in grant.kb_ids
    }
